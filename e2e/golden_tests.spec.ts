import { test, expect } from '@playwright/test'

test('test', async ({ page }) => {
  await page.goto('/')
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('button', { name: 'Ajouter une personne' }).click()
  await page.getByRole('textbox').nth(1).click({
    clickCount: 3
  })
  await page.getByRole('textbox').nth(1).fill('NIRINA')

  await page.getByLabel('Dépenseur').selectOption('1')
  await page.getByRole('listbox').selectOption('0')
  await page.getByRole('listbox').selectOption(['0', '1'])
  await page.getByRole('listbox').selectOption(['0', '1', '2'])
  await page.getByLabel('a dépensé').dblclick()
  await page.getByLabel('a dépensé').press('ArrowLeft')
  await page.getByLabel('a dépensé').fill('310')
  await page.getByRole('button', { name: 'Ajouter dépense' }).click()
  await page.getByLabel('Dépenseur').selectOption('2')
  await page.getByLabel('a dépensé').dblclick()
  await page.getByLabel('a dépensé').press('ControlOrMeta+a')
  await page.getByLabel('a dépensé').fill('218')
  await page.getByRole('button', { name: 'Ajouter dépense' }).click()

  await expect(page.getByLabel('balance de Un castor affairé')).toHaveValue('-176')
  await expect(page.getByLabel('balance de NIRINA')).toHaveValue('134')
  await expect(page.getByLabel('balance de Un ornithorynque malicieux')).toHaveValue('42')
  await expect(page.getByRole('row').nth(0)).toHaveText('Un castor affairé doit134€à NIRINA')
  await expect(page.getByRole('row').nth(1)).toHaveText(
    'Un castor affairé doit42€à Un ornithorynque malicieux'
  )
  await expect(
    page.getByText(
      'NIRINA a dépensé 310€ pour Un castor affairé, NIRINA, Un ornithorynque malicieux'
    )
  ).toBeVisible()

  await expect(
    page.getByText(
      'Un ornithorynque malicieux a dépensé 218€ pour Un castor affairé, NIRINA, Un ornithorynque malicieux'
    )
  ).toBeVisible()
})
