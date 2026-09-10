/**
 * `HeatmapRenderer`: draws one `HeatmapSource` into one canvas with WebGL2.
 *
 * Framework-free and DOM-free beyond its canvas: the caller owns the frame loop, sizing,
 * and the view (FRONTEND 3). Per frame it uploads only what changed (dirty depth columns
 * as texture rows, the column meta row, new trade instances) and issues five draw calls:
 * heatmap, bubbles, two outlined step lines, cursor. On context loss it stops drawing;
 * on restoration it rebuilds every GPU resource and re-uploads the whole source.
 */

import { bubbleReferenceContracts, BUBBLE_MAX_RADIUS_PX, BUBBLE_MIN_RADIUS_PX } from "./bubbles";
import { depthCeilingContracts, depthValue } from "./colorScale";
import { buildViridisLut } from "./colormap";
import { dirtyColumnRanges, heldBins, ringWriteRanges } from "./columns";
import { createProgram, createTexture, RendererError, type Program } from "./gl";
import {
  BUBBLE_FRAGMENT,
  BUBBLE_VERTEX,
  FULLSCREEN_VERTEX,
  HEATMAP_FRAGMENT,
  LINE_VERTEX,
  RECT_VERTEX,
  SOLID_FRAGMENT,
} from "./shaders";
import { TRADE_STRIDE, type DepthColumns, type HeatmapSource, type TradeInstances } from "./source";
import { leftBin, type PlotView } from "./viewport";

/** Colors shared with the legend and the CSS, as 0..1 sRGB. */
export const PALETTE = {
  bid: [0.3, 0.79, 0.94],
  ask: [0.97, 0.37, 0.64],
  cursor: [0.9, 0.93, 0.97],
} as const;

/** Beyond this many dirty ranges, re-upload the whole depth texture once. */
const MAX_UPLOAD_RANGES = 24;
const LUT_SIZE = 256;

export interface FrameStats {
  readonly drawn: boolean;
  readonly bubbles: number;
  readonly uploadedColumns: number;
}

interface Resources {
  readonly heatmap: Program;
  readonly line: Program;
  readonly bubble: Program;
  readonly rect: Program;
  readonly emptyVao: WebGLVertexArrayObject;
  readonly ramp: WebGLTexture;
}

interface SourceUpload {
  readonly columns: DepthColumns;
  readonly trades: TradeInstances;
  readonly depthTexture: WebGLTexture;
  readonly metaTexture: WebGLTexture;
  readonly uploadedRevisions: Uint32Array;
  readonly tradeBuffer: WebGLBuffer;
  readonly tradeVao: WebGLVertexArrayObject;
  uploadedTrades: number;
}

export class HeatmapRenderer {
  readonly #canvas: HTMLCanvasElement;
  readonly #gl: WebGL2RenderingContext;
  #resources: Resources | null = null;
  #upload: SourceUpload | null = null;
  #source: HeatmapSource | null = null;
  #dpr = 1;
  #lost = false;

  readonly #onLost = (event: Event): void => {
    event.preventDefault();
    this.#lost = true;
    this.#resources = null;
    this.#upload = null;
  };

  readonly #onRestored = (): void => {
    this.#lost = false;
    this.#resources = this.#createResources();
  };

  /**
   * @throws RendererError when WebGL2 is unavailable or the GPU lacks the texture size.
   */
  constructor(canvas: HTMLCanvasElement) {
    const gl = canvas.getContext("webgl2", {
      alpha: false,
      antialias: false,
      depth: false,
      stencil: false,
      premultipliedAlpha: true,
      powerPreference: "high-performance",
    });
    if (gl === null) throw new RendererError("WebGL2 is not available in this browser.");
    this.#canvas = canvas;
    this.#gl = gl;
    canvas.addEventListener("webglcontextlost", this.#onLost);
    canvas.addEventListener("webglcontextrestored", this.#onRestored);
    this.#resources = this.#createResources();
  }

  /** True between a context loss and its restoration. */
  get contextLost(): boolean {
    return this.#lost;
  }

  /** Sets what to draw; `null` draws an empty plot. */
  setSource(source: HeatmapSource | null): void {
    this.#source = source;
  }

  /** Sizes the drawing buffer to the canvas's CSS size at `devicePixelRatio`. */
  resize(cssWidth: number, cssHeight: number, devicePixelRatio: number): void {
    this.#dpr = Math.max(1, devicePixelRatio);
    const width = Math.max(1, Math.round(cssWidth * this.#dpr));
    const height = Math.max(1, Math.round(cssHeight * this.#dpr));
    if (this.#canvas.width !== width) this.#canvas.width = width;
    if (this.#canvas.height !== height) this.#canvas.height = height;
  }

  /** Draws one frame of `view`. */
  render(view: PlotView): FrameStats {
    const gl = this.#gl;
    const resources = this.#resources;
    if (this.#lost || resources === null || gl.isContextLost()) {
      return { drawn: false, bubbles: 0, uploadedColumns: 0 };
    }
    const width = this.#canvas.width;
    const height = this.#canvas.height;
    gl.viewport(0, 0, width, height);
    gl.disable(gl.BLEND);
    gl.clearColor(0.043, 0.051, 0.071, 1);
    gl.clear(gl.COLOR_BUFFER_BIT);
    const source = this.#source;
    if (source === null) return { drawn: true, bubbles: 0, uploadedColumns: 0 };

    const upload = this.#prepareUpload(source);
    const uploadedColumns = this.#uploadColumns(upload);
    this.#uploadTrades(upload);

    const frame: FrameUniforms = {
      viewportPx: [width, height],
      leftBin: leftBin(view),
      binsPerPx: view.visibleBins / width,
      nowBin: view.nowBin,
      priceLo: view.window.loE4,
      pricePerPx: (view.window.hiE4 - view.window.loE4) / height,
    };
    this.#drawHeatmap(resources, upload, source, frame);
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
    // Lines go over bubbles: a busy market's trade trail would otherwise hide the quotes.
    const bubbles = this.#drawBubbles(resources, upload, frame);
    this.#drawLines(resources, upload, frame);
    this.#drawCursor(resources, frame);
    return { drawn: true, bubbles, uploadedColumns };
  }

  /** Releases GPU resources and listeners. The canvas can be reused by a new renderer. */
  dispose(): void {
    this.#canvas.removeEventListener("webglcontextlost", this.#onLost);
    this.#canvas.removeEventListener("webglcontextrestored", this.#onRestored);
    this.#releaseUpload();
    const resources = this.#resources;
    this.#resources = null;
    if (resources === null || this.#gl.isContextLost()) return;
    const gl = this.#gl;
    for (const program of [resources.heatmap, resources.line, resources.bubble, resources.rect]) {
      gl.deleteProgram(program.program);
    }
    gl.deleteVertexArray(resources.emptyVao);
    gl.deleteTexture(resources.ramp);
  }

  #createResources(): Resources {
    const gl = this.#gl;
    const maxTexture = gl.getParameter(gl.MAX_TEXTURE_SIZE) as number;
    if (maxTexture < 2048) {
      throw new RendererError(`GPU texture limit ${maxTexture} is below 2048.`);
    }
    gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
    const ramp = createTexture(gl, gl.RGBA8, LUT_SIZE, 1, gl.LINEAR);
    gl.texSubImage2D(
      gl.TEXTURE_2D,
      0,
      0,
      0,
      LUT_SIZE,
      1,
      gl.RGBA,
      gl.UNSIGNED_BYTE,
      buildViridisLut(LUT_SIZE),
    );
    return {
      heatmap: createProgram(gl, FULLSCREEN_VERTEX, HEATMAP_FRAGMENT),
      line: createProgram(gl, LINE_VERTEX, SOLID_FRAGMENT),
      bubble: createProgram(gl, BUBBLE_VERTEX, BUBBLE_FRAGMENT),
      rect: createProgram(gl, RECT_VERTEX, SOLID_FRAGMENT),
      emptyVao: gl.createVertexArray(),
      ramp,
    };
  }

  /** Allocates GPU storage for `source`, reusing it while the source's arrays are the same. */
  #prepareUpload(source: HeatmapSource): SourceUpload {
    const current = this.#upload;
    if (current?.columns === source.columns && current.trades === source.trades) return current;
    this.#releaseUpload();
    const gl = this.#gl;
    const { columns, trades } = source;
    const tradeBuffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, tradeBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, trades.instances.byteLength, gl.DYNAMIC_DRAW);
    const tradeVao = gl.createVertexArray();
    gl.bindVertexArray(tradeVao);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, TRADE_STRIDE, gl.FLOAT, false, 0, 0);
    gl.vertexAttribDivisor(0, 1);
    gl.bindVertexArray(null);
    const uploadedRevisions = new Uint32Array(columns.capacity);
    // Differ from every live revision so the first frame uploads each column.
    uploadedRevisions.fill(0xffffffff);
    this.#upload = {
      columns,
      trades,
      depthTexture: createTexture(gl, gl.R32F, columns.rows, columns.capacity, gl.NEAREST),
      metaTexture: createTexture(gl, gl.RGBA32F, columns.capacity, 1, gl.NEAREST),
      uploadedRevisions,
      tradeBuffer,
      tradeVao,
      uploadedTrades: 0,
    };
    return this.#upload;
  }

  #releaseUpload(): void {
    const upload = this.#upload;
    this.#upload = null;
    if (upload === null || this.#gl.isContextLost()) return;
    const gl = this.#gl;
    gl.deleteTexture(upload.depthTexture);
    gl.deleteTexture(upload.metaTexture);
    gl.deleteBuffer(upload.tradeBuffer);
    gl.deleteVertexArray(upload.tradeVao);
  }

  #uploadColumns(upload: SourceUpload): number {
    const gl = this.#gl;
    const { columns } = upload;
    const ranges = dirtyColumnRanges(
      columns.revisions,
      upload.uploadedRevisions,
      MAX_UPLOAD_RANGES,
    );
    if (ranges.length === 0) return 0;
    gl.bindTexture(gl.TEXTURE_2D, upload.depthTexture);
    let uploaded = 0;
    for (const range of ranges) {
      gl.texSubImage2D(
        gl.TEXTURE_2D,
        0,
        0,
        range.start,
        columns.rows,
        range.count,
        gl.RED,
        gl.FLOAT,
        columns.depth,
        range.start * columns.rows,
      );
      uploaded += range.count;
    }
    gl.bindTexture(gl.TEXTURE_2D, upload.metaTexture);
    gl.texSubImage2D(
      gl.TEXTURE_2D,
      0,
      0,
      0,
      columns.capacity,
      1,
      gl.RGBA,
      gl.FLOAT,
      columns.meta,
      0,
    );
    upload.uploadedRevisions.set(columns.revisions);
    return uploaded;
  }

  #uploadTrades(upload: SourceUpload): void {
    const gl = this.#gl;
    const { trades } = upload;
    const ranges = ringWriteRanges(upload.uploadedTrades, trades.writeCount, trades.capacity);
    if (ranges.length === 0) return;
    gl.bindBuffer(gl.ARRAY_BUFFER, upload.tradeBuffer);
    for (const range of ranges) {
      gl.bufferSubData(
        gl.ARRAY_BUFFER,
        range.start * TRADE_STRIDE * Float32Array.BYTES_PER_ELEMENT,
        trades.instances,
        range.start * TRADE_STRIDE,
        range.count * TRADE_STRIDE,
      );
    }
    upload.uploadedTrades = trades.writeCount;
  }

  #drawHeatmap(
    resources: Resources,
    upload: SourceUpload,
    source: HeatmapSource,
    frame: FrameUniforms,
  ): void {
    const gl = this.#gl;
    const { program, uniforms } = resources.heatmap;
    const { columns } = upload;
    gl.useProgram(program);
    gl.bindVertexArray(resources.emptyVao);
    setView(gl, uniforms, frame);
    setRing(gl, uniforms, columns);
    bindTexture(gl, uniforms, "uMeta", 0, upload.metaTexture);
    bindTexture(gl, uniforms, "uDepth", 1, upload.depthTexture);
    bindTexture(gl, uniforms, "uRamp", 2, resources.ramp);
    gl.uniform1i(uniforms.get("uRows") ?? null, columns.rows);
    gl.uniform1f(uniforms.get("uRowStep") ?? null, columns.rowStepE4);
    gl.uniform1f(
      uniforms.get("uCeiling") ?? null,
      depthValue(depthCeilingContracts(source.maxRowContracts)),
    );
    gl.uniform1f(uniforms.get("uDpr") ?? null, this.#dpr);
    gl.uniform1i(uniforms.get("uEdgeStatus") ?? null, columns.edgeStatus);
    gl.drawArrays(gl.TRIANGLES, 0, 3);
  }

  #drawLines(resources: Resources, upload: SourceUpload, frame: FrameUniforms): void {
    const gl = this.#gl;
    const { program, uniforms } = resources.line;
    const { columns } = upload;
    const instances = Math.min(columns.capacity, heldBins(columns.headBin, columns.oldestBin));
    if (instances === 0) return;
    gl.useProgram(program);
    gl.bindVertexArray(resources.emptyVao);
    setView(gl, uniforms, frame);
    setRing(gl, uniforms, columns);
    bindTexture(gl, uniforms, "uMeta", 0, upload.metaTexture);
    const strokes: readonly (readonly [number, readonly number[], number, number])[] = [
      [0, [0, 0, 0], 0.75, 2.25],
      [1, [0, 0, 0], 0.75, 2.25],
      [0, PALETTE.bid, 1, 1.25],
      [1, PALETTE.ask, 1, 1.25],
    ];
    for (const [channel, rgb, alpha, halfWidthCss] of strokes) {
      gl.uniform1i(uniforms.get("uChannel") ?? null, channel);
      gl.uniform1f(uniforms.get("uHalfWidthPx") ?? null, halfWidthCss * this.#dpr);
      gl.uniform4f(uniforms.get("uColor") ?? null, rgb[0] ?? 0, rgb[1] ?? 0, rgb[2] ?? 0, alpha);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 12, instances);
    }
  }

  #drawBubbles(resources: Resources, upload: SourceUpload, frame: FrameUniforms): number {
    const gl = this.#gl;
    const { trades } = upload;
    const count = Math.min(trades.writeCount, trades.capacity);
    if (count === 0) return 0;
    const { program, uniforms } = resources.bubble;
    gl.useProgram(program);
    gl.bindVertexArray(upload.tradeVao);
    setView(gl, uniforms, frame);
    gl.uniform1f(uniforms.get("uReference") ?? null, bubbleReferenceContracts(trades.maxContracts));
    gl.uniform1f(uniforms.get("uMinRadiusPx") ?? null, BUBBLE_MIN_RADIUS_PX * this.#dpr);
    gl.uniform1f(uniforms.get("uMaxRadiusPx") ?? null, BUBBLE_MAX_RADIUS_PX * this.#dpr);
    gl.uniform1f(uniforms.get("uDpr") ?? null, this.#dpr);
    gl.uniform3f(uniforms.get("uBidColor") ?? null, ...PALETTE.bid);
    gl.uniform3f(uniforms.get("uAskColor") ?? null, ...PALETTE.ask);
    gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, count);
    gl.bindVertexArray(null);
    return count;
  }

  #drawCursor(resources: Resources, frame: FrameUniforms): void {
    const gl = this.#gl;
    const { program, uniforms } = resources.rect;
    const x = (frame.nowBin - frame.leftBin) / frame.binsPerPx;
    const halfWidth = 0.5 * this.#dpr;
    gl.useProgram(program);
    gl.bindVertexArray(resources.emptyVao);
    gl.uniform2f(uniforms.get("uViewportPx") ?? null, frame.viewportPx[0], frame.viewportPx[1]);
    gl.uniform4f(
      uniforms.get("uRectPx") ?? null,
      x - halfWidth,
      0,
      x + halfWidth,
      frame.viewportPx[1],
    );
    gl.uniform4f(uniforms.get("uColor") ?? null, ...PALETTE.cursor, 0.55);
    gl.drawArrays(gl.TRIANGLES, 0, 6);
  }
}

interface FrameUniforms {
  readonly viewportPx: readonly [number, number];
  readonly leftBin: number;
  readonly binsPerPx: number;
  readonly nowBin: number;
  readonly priceLo: number;
  readonly pricePerPx: number;
}

type Uniforms = ReadonlyMap<string, WebGLUniformLocation>;

function setView(gl: WebGL2RenderingContext, uniforms: Uniforms, frame: FrameUniforms): void {
  gl.uniform2f(uniforms.get("uViewportPx") ?? null, frame.viewportPx[0], frame.viewportPx[1]);
  gl.uniform1f(uniforms.get("uLeftBin") ?? null, frame.leftBin);
  gl.uniform1f(uniforms.get("uBinsPerPx") ?? null, frame.binsPerPx);
  gl.uniform1f(uniforms.get("uNowBin") ?? null, frame.nowBin);
  gl.uniform1f(uniforms.get("uPriceLo") ?? null, frame.priceLo);
  gl.uniform1f(uniforms.get("uPricePerPx") ?? null, frame.pricePerPx);
}

function setRing(gl: WebGL2RenderingContext, uniforms: Uniforms, columns: DepthColumns): void {
  gl.uniform1i(uniforms.get("uCapacity") ?? null, columns.capacity);
  gl.uniform1i(uniforms.get("uHeadBin") ?? null, columns.headBin);
  gl.uniform1i(uniforms.get("uOldestBin") ?? null, columns.oldestBin);
}

function bindTexture(
  gl: WebGL2RenderingContext,
  uniforms: Uniforms,
  name: string,
  unit: number,
  texture: WebGLTexture,
): void {
  gl.activeTexture(gl.TEXTURE0 + unit);
  gl.bindTexture(gl.TEXTURE_2D, texture);
  gl.uniform1i(uniforms.get(name) ?? null, unit);
}
