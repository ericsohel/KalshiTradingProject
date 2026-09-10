import js from "@eslint/js";
import { defineConfig, globalIgnores } from "eslint/config";
import reactHooks from "eslint-plugin-react-hooks";
import tseslint from "typescript-eslint";

const frameworkFree = {
  group: ["react", "react/*", "react-dom", "react-dom/*"],
  message: "Only src/ui may depend on React (docs/FRONTEND.md section 3).",
};

// ESLint loads its flat config from the module's default export; config files are the
// only modules allowed one (docs/ENGINEERING_STANDARDS.md section 6).
export default defineConfig([
  globalIgnores(["dist/", "node_modules/", "coverage/"]),
  {
    files: ["**/*.{ts,tsx}"],
    extends: [js.configs.recommended, tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    rules: {
      "no-restricted-exports": [
        "error",
        {
          restrictDefaultExports: {
            direct: true,
            named: true,
            defaultFrom: true,
            namedFrom: true,
            namespaceFrom: true,
          },
        },
      ],
      "no-console": "error",
      eqeqeq: "error",
      "@typescript-eslint/no-non-null-assertion": "error",
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/switch-exhaustiveness-check": "error",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
    },
  },
  {
    files: ["src/ui/**/*.{ts,tsx}", "src/main.tsx"],
    extends: [reactHooks.configs.flat.recommended],
  },
  {
    files: ["src/api/**", "src/state/**"],
    rules: { "no-restricted-imports": ["error", { patterns: [frameworkFree] }] },
  },
  {
    files: ["src/render/**"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            frameworkFree,
            {
              group: ["../ui/*", "../state/*", "../api/*"],
              message: "The renderer reads a HeatmapSource, not the app.",
            },
          ],
        },
      ],
      "no-restricted-globals": [
        "error",
        { name: "window", message: "The renderer touches nothing beyond its canvas." },
        { name: "document", message: "The renderer touches nothing beyond its canvas." },
        { name: "requestAnimationFrame", message: "The caller owns the frame loop." },
      ],
    },
  },
  {
    files: ["*.config.{js,ts}"],
    rules: { "no-restricted-exports": "off" },
  },
  {
    files: ["dev/**"],
    rules: { "no-console": "off" },
  },
  {
    files: ["**/*.js"],
    extends: [tseslint.configs.disableTypeChecked],
  },
]);
