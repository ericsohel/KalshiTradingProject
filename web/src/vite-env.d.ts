/** Build-time settings read through `import.meta.env`. */
interface ImportMetaEnv {
  /** Absolute origin of `tape serve` when the page is hosted elsewhere; unset for same-origin. */
  readonly VITE_TAPE_API_ORIGIN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
