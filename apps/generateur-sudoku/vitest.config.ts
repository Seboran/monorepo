import { fileURLToPath } from 'node:url'
import {
  mergeConfig,
  defineConfig,
  configDefaults,
  defineConfig,
} from 'vitest/config'
import viteConfig from './vite.config'

export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: 'happy-dom',
      globals: true,
      exclude: [...configDefaults.exclude, 'e2e/*'],
      root: fileURLToPath(new URL('./', import.meta.url)),
    },
  }),
)