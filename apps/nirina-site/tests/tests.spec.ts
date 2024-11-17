import { test, expect } from '@playwright/test'

test('has title', async ({ page }) => {
  await page.goto('/')

  // Expect a title "to contain" a substring.
  await expect(page).toHaveTitle(/Nirina Rabeson/)
})

test('get started link', async ({ page }) => {
  await page.goto('/')

  // Click the get started link.
  await page.getByRole('link', { name: 'Me contacter' }).click()
  await page.getByRole('link', { name: /LinkedIn/ }).click()

  // Expects page to have a heading with the name of Installation.
  await expect(page).toHaveURL(/https:\/\/www.linkedin.com\/*/)
})

test("affiche le titre d'un article et permet de cliquer dessus", async ({
  page,
}) => {
  await page.goto('/')
  const textContent = await page
    .getByRole('heading')
    .getByRole('link')
    .first()
    .textContent()
  await page.getByRole('heading').getByRole('link').first().click()

  await expect(page).toHaveURL(/\/posts\/*/)
  await expect(page).toHaveTitle(new RegExp(textContent! + ' | Nirina Rabeson'))
})

test('affiche 404', async ({ page }) => {
  await page.goto('/jemesuisperduetjedevraispasetreici2388394')
  await expect(page.getByText('404')).toBeVisible()
})