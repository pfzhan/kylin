const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
const { clearFormInput } = require('../utils/domHelper')
const { closeLicenseBox, waitingForPageClean } = require('../utils/businessHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN,
  USERNAME_PROJECT_ADMIN,
  PASSWORD_PROJECT_ADMIN,
  USERNAME_PROJECT_MANAGEMENT,
  PASSWORD_PROJECT_MANAGEMENT,
  USERNAME_PROJECT_OPERATION,
  PASSWORD_PROJECT_OPERATION,
  USERNAME_PROJECT_QUERY,
  PASSWORD_PROJECT_QUERY
} = process.env

/* eslint-disable newline-per-chained-call */
describe('各个角色的登录登出', async function () {
  this.timeout(30000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  it('空的表单登录', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)

    await driver.findElement(By.css('.login-form .el-button--primary')).click()
    await driver.sleep(1000)

    const usernameString = await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) .el-form-item__error')).getText()
    assert.equal(usernameString, '请输入用户名')

    const passwordString = await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) .el-form-item__error')).getText()
    assert.equal(passwordString, '请输入密码')
  })

  it('用户管理员登录', async () => {
    await waitingForPageClean(driver)
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_ADMIN)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_ADMIN)

    await closeLicenseBox(driver)
  })

  it('用户管理员登出', async () => {
    const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
    const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
    await driver.actions().move({ origin: usernameEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

    await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
    await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

    await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
  })

  it('项目管理员登录', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_PROJECT_ADMIN)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_PROJECT_ADMIN)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_PROJECT_ADMIN)

    await closeLicenseBox(driver)
  })

  it('项目管理员登出', async () => {
    const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
    const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
    await driver.actions().move({ origin: usernameEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

    await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
    await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

    await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
  })

  it('Management登录', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_PROJECT_MANAGEMENT)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_PROJECT_MANAGEMENT)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_PROJECT_MANAGEMENT)

    await closeLicenseBox(driver)
  })

  it('Management登出', async () => {
    const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
    const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
    await driver.actions().move({ origin: usernameEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

    await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
    await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

    await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
  })

  it('Operation登录', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_PROJECT_OPERATION)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_PROJECT_OPERATION)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_PROJECT_OPERATION)

    await closeLicenseBox(driver)
  })

  it('Operation登出', async () => {
    const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
    const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
    await driver.actions().move({ origin: usernameEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

    await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
    await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

    await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
  })

  it('Query登录', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_PROJECT_QUERY)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(PASSWORD_PROJECT_QUERY)
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
    assert.equal(await driver.findElement(By.css('.topbar .limit-user-name')).getText(), USERNAME_PROJECT_QUERY)

    await closeLicenseBox(driver)
  })

  it('Query登出', async () => {
    const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
    const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
    await driver.actions().move({ origin: usernameEl }).perform()
    await driver.sleep(1000)

    await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
    await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

    await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
    await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

    await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
  })

  it('错误的用户名', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys('error_error_error')
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys('error_error_error')
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.errMsgBox'))), 10000)
    const errorString = await driver.findElement(By.css('.errMsgBox .error-title')).getText()
    assert.equal(errorString.includes('找不到用户'), true)

    await driver.executeScript(`
      var button = document.querySelector(".errMsgBox .dialog-footer .el-button--default")
      button.dispatchEvent(new Event("click"))
    `)
  })

  it('错误的密码', async () => {
    await waitingForPageClean(driver)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(USERNAME_ADMIN)
    await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(2) input')
    await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys('error_error_error')
    await driver.findElement(By.css('.login-form .el-button--primary')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.errMsgBox .error-title'))), 10000)
    const errorString = await driver.findElement(By.css('.errMsgBox .error-title')).getText()
    assert.equal(errorString.includes('用户名或密码错误。'), true)

    await driver.executeScript(`
      var button = document.querySelector(".errMsgBox .dialog-footer .el-button--default")
      button.dispatchEvent(new Event("click"))
    `)
  })
})
