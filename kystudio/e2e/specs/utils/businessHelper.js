const { until, By } = require('selenium-webdriver')
const { waitingForStable } = require('../utils/domHelper')

exports.closeLicenseBox = async function closeLicenseBox (driver) {
  try {
    await driver.findElement(By.css('.el-dialog__wrapper.linsencebox')).click()
  } catch (e) {}
}

exports.waitingForPageClean = async function waitingForPageClean (driver) {
  try {
    const messageBoxWrappers = await driver.findElements(By.css('.el-message-box__wrapper'))
    for (const messageBoxWrapper of messageBoxWrappers) {
      try {
        await driver.wait(until.elementIsNotVisible(messageBoxWrapper), 10000)
      } catch (e) {}
    }
  } catch (e) {}

  try {
    const messageBoxWrappers = await driver.findElements(By.css('.el-dialog__wrapper'))
    for (const messageBoxWrapper of messageBoxWrappers) {
      try {
        await driver.wait(until.elementIsNotVisible(messageBoxWrapper), 10000)
      } catch (e) {}
    }
  } catch (e) {}
}

// 封装的登录
exports.login = async function login(driver, username, password) {
  await driver.wait(until.elementLocated(By.css('.login-form .el-button--primary')), 10000)

  await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(1) input')).sendKeys(username)
  await driver.findElement(By.css('.login-form .input_group .el-form-item:nth-child(2) input')).sendKeys(password)
  await driver.findElement(By.css('.login-form .el-button--primary')).click()

  await driver.wait(until.elementLocated(By.css('.topbar .limit-user-name')), 10000)
}

// 封装的登出
exports.logout = async function logout(driver) {
  const usernameEl = await driver.findElement(By.css('.topbar .user-msg-dropdown .el-dropdown-link'))
  const dropdownMenuId = await usernameEl.getAttribute('aria-controls')
  await driver.actions().move({ origin: usernameEl }).perform()
  await driver.sleep(1000)

  await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
  await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(3)`)).click()

  await driver.wait(until.elementLocated(By.css('.el-message-box')), 10000)
  await driver.findElement(By.css(`.el-message-box .el-button--primary`)).click()

  await driver.wait(until.elementLocated(By.css('.login-form')), 10000)
}
