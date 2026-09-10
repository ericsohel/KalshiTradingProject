/**
 * Small WebGL2 helpers: compiling programs, looking up uniforms, and allocating textures
 * with the parameters data textures need.
 */

export class RendererError extends Error {
  override readonly name = "RendererError";
}

export interface Program {
  readonly program: WebGLProgram;
  readonly uniforms: ReadonlyMap<string, WebGLUniformLocation>;
}

function compileShader(gl: WebGL2RenderingContext, type: GLenum, source: string): WebGLShader {
  const shader = gl.createShader(type);
  if (shader === null) throw new RendererError("createShader failed");
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (gl.getShaderParameter(shader, gl.COMPILE_STATUS) !== true && !gl.isContextLost()) {
    const log = gl.getShaderInfoLog(shader) ?? "no log";
    gl.deleteShader(shader);
    throw new RendererError(`Shader compilation failed: ${log}`);
  }
  return shader;
}

/**
 * Compiles and links a program and resolves its active uniforms by name.
 *
 * @throws RendererError when compilation or linking fails on a live context.
 */
export function createProgram(
  gl: WebGL2RenderingContext,
  vertexSource: string,
  fragmentSource: string,
): Program {
  const vertex = compileShader(gl, gl.VERTEX_SHADER, vertexSource);
  const fragment = compileShader(gl, gl.FRAGMENT_SHADER, fragmentSource);
  const program = gl.createProgram();
  gl.attachShader(program, vertex);
  gl.attachShader(program, fragment);
  gl.linkProgram(program);
  gl.deleteShader(vertex);
  gl.deleteShader(fragment);
  if (gl.getProgramParameter(program, gl.LINK_STATUS) !== true && !gl.isContextLost()) {
    const log = gl.getProgramInfoLog(program) ?? "no log";
    gl.deleteProgram(program);
    throw new RendererError(`Program link failed: ${log}`);
  }
  const uniforms = new Map<string, WebGLUniformLocation>();
  const count = gl.getProgramParameter(program, gl.ACTIVE_UNIFORMS) as number;
  for (let index = 0; index < count; index += 1) {
    const info = gl.getActiveUniform(program, index);
    if (info === null) continue;
    const location = gl.getUniformLocation(program, info.name);
    if (location !== null) uniforms.set(info.name, location);
  }
  return { program, uniforms };
}

/**
 * A texture with immutable storage. Data textures use NEAREST sampling: filtering float
 * textures needs an extension WebGL2 does not guarantee, and the shaders use `texelFetch`.
 */
export function createTexture(
  gl: WebGL2RenderingContext,
  internalFormat: GLenum,
  width: number,
  height: number,
  filter: GLenum,
): WebGLTexture {
  const texture = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, texture);
  gl.texStorage2D(gl.TEXTURE_2D, 1, internalFormat, width, height);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, filter);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, filter);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
  return texture;
}
