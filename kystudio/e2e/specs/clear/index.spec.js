const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
const { login, logout } = require('../utils/businessHelper');

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN,
  PROJECT_NAME
} = process.env

/* eslint-disable newline-per-chained-call */
describe('系统管理员进入系统', async function () {
  this.timeout(30000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  it('删除自动化测试创建的用户', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)
    // 统一调用登录
    await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN);
    await driver.sleep(3000)

  })

  it('删除模型', async () => {

  })

  it('删除项目', async () => {

  })
})
