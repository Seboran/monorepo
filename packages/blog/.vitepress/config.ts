import { defineConfig } from 'vitepress'

const titleCard = 'Blog de Nirina Rabeson'
const descriptionCard = 'homepage'
const imageurlCard = 'https://www.nirinarabeson.fr/card.png'
export default defineConfig({
  title: 'Nirina Rabeson',
  cleanUrls: true,
  head: [
    [
      'meta',
      {
        property: 'og:image:secure_url',
        content: imageurlCard
      }
    ],
    [
      'meta',
      {
        property: 'og:image',
        content: imageurlCard
      }
    ],
    [
      'meta',
      {
        property: 'og:image:url',
        content: imageurlCard
      }
    ],
    ['meta', { name: 'twitter:site', content: '@nirinarabeson.fr' }],
    ['meta', { name: 'twitter:title', content: titleCard }],
    ['meta', { property: 'og:title', content: titleCard }],
    [
      'meta',
      {
        name: 'twitter:image',
        content: imageurlCard
      }
    ],
    ['meta', { property: 'og:type', content: 'homepage' }],
    ['meta', { name: 'twitter:description', content: descriptionCard }],
    ['meta', { property: 'og:description', content: descriptionCard }],
    ['description', { content: 'Le site de Nirina Rabeson' }],
    [
      'link',
      {
        rel: 'icon',
        type: 'image/x-icon',
        href: `data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>✨</text></svg>`
      }
    ]
  ],
  description: 'Site web et blog de Nirina Rabeson',
  appearance: false,
  metaChunk: true,
  vite: {
    build: {
      assetsInlineLimit: 0
    }
  }
})
