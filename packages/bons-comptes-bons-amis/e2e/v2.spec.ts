import { test, expect } from '@playwright/test'

test('test-toute-utilisation', async ({ page }) => {
  await page.goto('/')
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByLabel('dépense de Un castor affairé').getByRole('spinbutton').click()
  await page.getByLabel('dépense de Un castor affairé').getByRole('spinbutton').fill('50')
  await page.getByLabel('dépense de Une autruche curieuse').getByRole('spinbutton').click()
  await page.getByLabel('dépense de Une autruche curieuse').getByRole('spinbutton').fill('31')
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByLabel('dépense de Un ornithorynque malicieux').getByRole('spinbutton').click()
  await page.getByLabel('dépense de Un ornithorynque malicieux').getByRole('spinbutton').fill('200')
  await page.getByLabel('dépense de Un koala gourmand').getByRole('spinbutton').click()
  await page.getByLabel('dépense de Un koala gourmand').getByRole('spinbutton').fill('21')
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Calculer remboursements' }).click()
  await expect(
    page.locator('section').filter({ hasText: 'quidoità qui' }).getByRole('row')
  ).toHaveText([
    'quidoità qui',
    'Une autruche curieuse6.86€Un castor affairé',
    'Une autruche curieuse5.29€Un ornithorynque malicieux',
    'Un paresseux rêveur43.14€Un ornithorynque malicieux',
    'Un koala gourmand22.14€Un ornithorynque malicieux',
    'Un panda joueur43.14€Un ornithorynque malicieux',
    'Un loup solitaire43.14€Un ornithorynque malicieux'
  ])
})
