/**
 * GLSL ES 3.00 sources for the heatmap, the best bid and ask lines, trade bubbles, and
 * the time cursor.
 *
 * Shared conventions: `gl_FragCoord` and every `*Px` uniform are device pixels with the
 * origin at the bottom left; time is in bins (`uLeftBin` at x = 0, `uBinsPerPx` per
 * pixel); price is in e4 (`uPriceLo` at y = 0, `uPricePerPx` per pixel). Integer `%` is
 * only ever applied to non-negative operands, where GLSL ES defines it.
 */

const HEADER = `#version 300 es
precision highp float;
precision highp int;
precision highp sampler2D;
`;

const VIEW_UNIFORMS = `
uniform vec2 uViewportPx;
uniform float uLeftBin;
uniform float uBinsPerPx;
uniform float uNowBin;
uniform float uPriceLo;
uniform float uPricePerPx;
`;

const RING_UNIFORMS = `
uniform sampler2D uMeta;
uniform int uCapacity;
uniform int uHeadBin;
uniform int uOldestBin;
`;

/** Covers the viewport with one triangle, no attributes. */
export const FULLSCREEN_VERTEX = `${HEADER}
void main() {
  vec2 corner = vec2(float((gl_VertexID << 1) & 2), float(gl_VertexID & 2));
  gl_Position = vec4(corner * 2.0 - 1.0, 0.0, 1.0);
}
`;

/**
 * The heatmap. Each device pixel covers a span of bins and rows; it takes the maximum
 * depth over up to SAMPLES x SAMPLES texels of that span (a thin level never disappears
 * between pixels) and the worst status of the columns it covers. Bins after the head, up
 * to now, continue the head column's depth with uEdgeStatus, the book's state now, as
 * sampleBin in columns.ts does. Stale columns keep their depth under amber hatching;
 * unknown and gap columns draw grey hatching on the background, gap more strongly. Time
 * after now is plain background.
 */
export const HEATMAP_FRAGMENT = `${HEADER}${VIEW_UNIFORMS}${RING_UNIFORMS}
uniform sampler2D uDepth;
uniform sampler2D uRamp;
uniform int uRows;
uniform float uRowStep;
uniform float uCeiling;
uniform float uDpr;
uniform int uEdgeStatus;

out vec4 outColor;

const int SAMPLES = 6;
const vec3 BACKGROUND = vec3(0.043, 0.051, 0.071);
const vec3 OUTSIDE = vec3(0.027, 0.031, 0.043);
const vec3 GAP_INK = vec3(0.42, 0.46, 0.54);
const vec3 STALE_INK = vec3(0.97, 0.72, 0.33);

float stripes(float along) {
  float spacing = 8.0 * uDpr;
  return 1.0 - step(1.25 * uDpr, mod(along, spacing));
}

void main() {
  vec2 pixel = floor(gl_FragCoord.xy);
  float binLo = uLeftBin + pixel.x * uBinsPerPx;
  if (binLo >= uNowBin) {
    outColor = vec4(BACKGROUND, 1.0);
    return;
  }
  float binHi = min(binLo + uBinsPerPx, uNowBin);
  float priceLo = uPriceLo + pixel.y * uPricePerPx;
  float priceHi = priceLo + uPricePerPx;
  float halfRow = 0.5 * uRowStep;
  int rowLo = max(0, int(floor((priceLo + halfRow) / uRowStep)));
  int rowHi = min(uRows - 1, int(ceil((priceHi + halfRow) / uRowStep)) - 1);
  bool onGrid = priceHi > -halfRow && priceLo < 10000.0 + halfRow && rowLo <= rowHi;

  int binFirst = int(floor(binLo));
  int binLast = max(binFirst, int(ceil(binHi)) - 1);
  int binStride = max(1, (binLast - binFirst + SAMPLES) / SAMPLES);
  int rowStride = max(1, (rowHi - rowLo + SAMPLES) / SAMPLES);

  float depth = 0.0;
  int status = 0;
  for (int i = 0; i < SAMPLES; i++) {
    int bin = binFirst + i * binStride;
    if (bin > binLast) break;
    if (uHeadBin < 0 || bin < uOldestBin) {
      status = max(status, 2);
      continue;
    }
    int column = min(bin, uHeadBin) % uCapacity;
    int columnStatus = bin > uHeadBin
      ? uEdgeStatus
      : int(texelFetch(uMeta, ivec2(column, 0), 0).b + 0.5);
    status = max(status, columnStatus);
    if (!onGrid || columnStatus >= 2) continue;
    for (int j = 0; j < SAMPLES; j++) {
      int row = rowLo + j * rowStride;
      if (row > rowHi) break;
      depth = max(depth, texelFetch(uDepth, ivec2(row, column), 0).r);
    }
  }

  vec3 color = onGrid ? BACKGROUND : OUTSIDE;
  if (depth > 0.0) {
    float t = clamp(depth / uCeiling, 0.0, 1.0);
    color = texture(uRamp, vec2(t * (255.0 / 256.0) + 0.5 / 256.0, 0.5)).rgb;
  }
  if (status == 1) {
    color = mix(color, STALE_INK, 0.6 * stripes(gl_FragCoord.x + gl_FragCoord.y));
  } else if (status >= 2) {
    float strength = status == 3 ? 0.55 : 0.28;
    color = mix(onGrid ? BACKGROUND : OUTSIDE, GAP_INK, strength * stripes(gl_FragCoord.x - gl_FragCoord.y));
  }
  outColor = vec4(color, 1.0);
}
`;

/**
 * Best bid or ask as a step line, one instance per held bin (instance 0 is the head),
 * with no vertex attributes: prices come from the column meta texture. Vertices 0-5 draw
 * the bin's horizontal segment, 6-11 the vertical riser from the previous bin's price.
 * The head bin's segment extends to now.
 */
export const LINE_VERTEX = `${HEADER}${VIEW_UNIFORMS}${RING_UNIFORMS}
uniform int uChannel;
uniform float uHalfWidthPx;

const vec2 QUAD[6] = vec2[6](vec2(0.0, 0.0), vec2(1.0, 0.0), vec2(0.0, 1.0),
                             vec2(0.0, 1.0), vec2(1.0, 0.0), vec2(1.0, 1.0));
const vec4 HIDDEN = vec4(-2.0, -2.0, 0.0, 1.0);

float priceAt(int bin) {
  if (bin < 0 || bin < uOldestBin || bin > uHeadBin) return -1.0;
  vec4 meta = texelFetch(uMeta, ivec2(bin % uCapacity, 0), 0);
  return uChannel == 0 ? meta.r : meta.g;
}

vec4 toClip(vec2 px) {
  return vec4(px / uViewportPx * 2.0 - 1.0, 0.0, 1.0);
}

void main() {
  int bin = uHeadBin - gl_InstanceID;
  float price = priceAt(bin);
  if (price < 0.0) {
    gl_Position = HIDDEN;
    return;
  }
  vec2 corner = QUAD[gl_VertexID % 6];
  float x0 = (float(bin) - uLeftBin) / uBinsPerPx;
  float binEnd = bin == uHeadBin ? max(uNowBin, float(bin)) : float(bin) + 1.0;
  float x1 = (binEnd - uLeftBin) / uBinsPerPx;
  float y = (price - uPriceLo) / uPricePerPx;
  if (gl_VertexID < 6) {
    gl_Position = toClip(vec2(mix(x0, x1, corner.x), y + (corner.y * 2.0 - 1.0) * uHalfWidthPx));
    return;
  }
  float previous = priceAt(bin - 1);
  if (previous < 0.0 || previous == price) {
    gl_Position = HIDDEN;
    return;
  }
  float yPrevious = (previous - uPriceLo) / uPricePerPx;
  float low = min(y, yPrevious) - uHalfWidthPx;
  float high = max(y, yPrevious) + uHalfWidthPx;
  gl_Position = toClip(vec2(x0 + (corner.x * 2.0 - 1.0) * uHalfWidthPx, mix(low, high, corner.y)));
}
`;

export const SOLID_FRAGMENT = `${HEADER}
uniform vec4 uColor;
out vec4 outColor;
void main() {
  outColor = vec4(uColor.rgb * uColor.a, uColor.a);
}
`;

/**
 * Trade bubbles, one instance per trade: attribute aTrade = (bin, price e4, contracts,
 * side). The radius formula mirrors bubbleRadiusPx in bubbles.ts.
 */
export const BUBBLE_VERTEX = `${HEADER}${VIEW_UNIFORMS}
layout(location = 0) in vec4 aTrade;
uniform float uReference;
uniform float uMinRadiusPx;
uniform float uMaxRadiusPx;
uniform float uDpr;
out vec2 vLocal;
out float vRadius;
out float vSide;

const vec2 CORNERS[6] = vec2[6](vec2(-1.0, -1.0), vec2(1.0, -1.0), vec2(-1.0, 1.0),
                                vec2(-1.0, 1.0), vec2(1.0, -1.0), vec2(1.0, 1.0));

void main() {
  float contracts = aTrade.z;
  float x = (aTrade.x - uLeftBin) / uBinsPerPx;
  float radius = clamp(uMaxRadiusPx * sqrt(max(contracts, 0.0) / uReference), uMinRadiusPx, uMaxRadiusPx);
  if (contracts <= 0.0 || x < -radius - 2.0) {
    gl_Position = vec4(-2.0, -2.0, 0.0, 1.0);
    return;
  }
  vec2 center = vec2(x, (aTrade.y - uPriceLo) / uPricePerPx);
  vec2 corner = CORNERS[gl_VertexID];
  float extent = radius + uDpr;
  vLocal = corner * extent;
  vRadius = radius;
  vSide = aTrade.w;
  gl_Position = vec4((center + vLocal) / uViewportPx * 2.0 - 1.0, 0.0, 1.0);
}
`;

export const BUBBLE_FRAGMENT = `${HEADER}
in vec2 vLocal;
in float vRadius;
in float vSide;
uniform vec3 uBidColor;
uniform vec3 uAskColor;
uniform float uDpr;
out vec4 outColor;

void main() {
  float distance = length(vLocal);
  float coverage = clamp(vRadius - distance + 0.5, 0.0, 1.0);
  if (coverage <= 0.0) discard;
  vec3 fill = vSide < 0.5 ? uBidColor : uAskColor;
  float rim = smoothstep(vRadius - 1.6 * uDpr, vRadius - 0.6 * uDpr, distance);
  vec3 color = mix(fill * 0.9, mix(fill, vec3(1.0), 0.35), rim);
  float alpha = coverage * mix(0.42, 0.95, rim);
  outColor = vec4(color * alpha, alpha);
}
`;

/** A rectangle in device pixels, for the time cursor. */
export const RECT_VERTEX = `${HEADER}
uniform vec2 uViewportPx;
uniform vec4 uRectPx;
const vec2 QUAD[6] = vec2[6](vec2(0.0, 0.0), vec2(1.0, 0.0), vec2(0.0, 1.0),
                             vec2(0.0, 1.0), vec2(1.0, 0.0), vec2(1.0, 1.0));
void main() {
  vec2 corner = QUAD[gl_VertexID];
  vec2 px = mix(uRectPx.xy, uRectPx.zw, corner);
  gl_Position = vec4(px / uViewportPx * 2.0 - 1.0, 0.0, 1.0);
}
`;
