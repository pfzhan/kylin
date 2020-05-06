const { until, By } = require('selenium-webdriver')

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
