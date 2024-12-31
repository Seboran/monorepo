import type { AstroIntegration } from 'astro'
import { parse } from 'node-html-parser'
import { readFile, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import * as crypto from 'crypto'

const NETLIFY_CONFIG_PATH = './netlify.toml' // Adjust this path as necessary

const createCspHash = async (scriptContent: string): Promise<string> => {
  const hash = crypto
    .createHash('sha256')
    .update(scriptContent)
    .digest('base64')
  return `'sha256-${hash}'`
}

const updateNetlifyCSP = async (
  scriptHashes: string[],
  styleHashes: string[],
): Promise<void> => {
  try {
    // Read the Netlify configuration file
    const configContent = await readFile(NETLIFY_CONFIG_PATH, 'utf-8')

    // Extract and update the CSP header
    const updatedConfig = configContent.replace(
      /Content-Security-Policy\s*=\s*"([^"]+)"/,
      (match, currentCsp) => {
        const cspParts = currentCsp.split(';').map((part: any) => part.trim())
        const scriptSrcIndex = cspParts.findIndex((part: any) =>
          part.startsWith('script-src'),
        )

        if (scriptSrcIndex !== -1) {
          cspParts[scriptSrcIndex] =
            `script-src 'self' ${scriptHashes.join(' ')}`
        } else {
          cspParts.push(`script-src 'self' ${scriptHashes.join(' ')}`)
        }

        // Update or add style-src
        const styleSrcIndex = cspParts.findIndex((part: any) =>
          part.startsWith('style-src'),
        )
        const hashesWithGoogle = [...styleHashes, 'fonts.googleapis.com']
        if (styleSrcIndex !== -1) {
          cspParts[styleSrcIndex] =
            `style-src 'self' ${hashesWithGoogle.join(' ')}`
        } else {
          cspParts.push(`style-src 'self' ${hashesWithGoogle.join(' ')}`)
        }

        return `Content-Security-Policy = "${cspParts.join('; ')}"`
      },
    )

    // Write the updated configuration back to the file
    await writeFile(NETLIFY_CONFIG_PATH, updatedConfig, 'utf-8')
    console.log(
      'Netlify configuration updated successfully with new CSP hashes.',
    )
  } catch (error) {
    console.error(`Error updating Netlify configuration: ${error}`)
  }
}

export const astroCSPHashGenerator: AstroIntegration = {
  name: 'astro-csp-hash-generator',
  hooks: {
    'astro:build:done': async ({ dir, pages, logger }) => {
      const scriptHashes: string[] = []
      const styleHashes: string[] = []

      for (const page of pages) {
        const filePath = fileURLToPath(`${dir.href}${page.pathname}index.html`)

        try {
          const root = parse(await readFile(filePath, { encoding: 'utf-8' }))
          const scripts = root.querySelectorAll('script')

          for (const script of scripts) {
            const hash = await createCspHash(script.textContent || '')
            scriptHashes.push(hash)
          }

          // Process style tags
          const styles = root.querySelectorAll('style')
          for (const style of styles) {
            const hash = await createCspHash(style.textContent || '')
            styleHashes.push(hash)
          }
        } catch (error) {
          logger.error(`Cannot read file ${filePath}: ${error}`)
        }
      }

      if (scriptHashes.length > 0 || styleHashes.length > 0) {
        await updateNetlifyCSP(scriptHashes, styleHashes)
      }

      logger.info(`Generated hashes: ${scriptHashes.join(' ')}`)
    },
  },
}