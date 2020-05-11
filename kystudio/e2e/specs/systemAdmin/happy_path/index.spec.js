const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
const { login, logout } = require('../../utils/businessHelper')
const { changeFormInput, changeFormTextarea } = require('../../utils/domHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN,
  PROJECT_NAME,
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
describe('系统管理员创建项目', async function () {
  this.timeout(30000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  // 创建项目
  it('创建项目', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)

    // 统一调用登录
    await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN);
    await driver.sleep(2000)

    await driver.findElement(By.css('.topbar .add-project-btn')).click()
    await driver.sleep(1000)

    changeFormInput(driver, '.project-edit-modal .el-dialog__body .el-form-item:nth-child(2)', PROJECT_NAME)
    changeFormTextarea(driver, '.project-edit-modal .el-dialog__body .el-form-item:nth-child(3)', 'this is it test auto create project')
    await driver.sleep(1000)
    await driver.findElement(By.css('.project-edit-modal .el-dialog__footer .el-button--primary')).click()

    await driver.sleep(5000)
    // 添加完成后，进入的是数据源页面
    assert.equal(await driver.getCurrentUrl(), `${LAUNCH_URL}/#/studio/source`);
  })

  /*
  // 加载数据源
  it('数据源', async () => {

  })

  // 设置默认数据库
  it('配置默认数据库', async () => {

  })

  // 进入系统管理页面
  it('进入系统管理页面', async () => {
    await driver.findElement(By.css('.entry-admin')).click()
    await driver.sleep(2000)
    // todo 执行进入用户页面
  })

  // 设置创建项目 admin 用户
  it('设置创建项目 admin 用户 it_p_admin', async () => {
    await driver.findElement(By.css('.security-user .el-row .el-button--primary')).click()
    await driver.sleep(1000)

    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(1)', USERNAME_PROJECT_ADMIN)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(2)', PASSWORD_PROJECT_ADMIN)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(3)', PASSWORD_PROJECT_ADMIN)
    
    await driver.findElement(By.css('.user-edit-modal .el-dialog__footer:nth-child(3) .el-button--primary')).click()

    await driver.sleep(5000)

    // todo 验证存在这个用户
  })

  // 设置创建项目 management 用户
  it('设置创建项目 management 用户 it_p_m', async () => {
    await driver.findElement(By.css('.security-user .el-row .el-button--primary')).click()
    await driver.sleep(1000)

    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(1)', USERNAME_PROJECT_MANAGEMENT)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(2)', PASSWORD_PROJECT_MANAGEMENT)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(3)', PASSWORD_PROJECT_MANAGEMENT)

    await driver.findElement(By.css('.user-edit-modal .el-dialog__footer:nth-child(3) .el-button--primary')).click()

    await driver.sleep(5000)

    // todo 验证存在这个用户
  })

  // 设置创建项目 operator 用户
  it('设置创建项目 operator 用户 it_p_o', async () => {
    await driver.findElement(By.css('.security-user .el-row .el-button--primary')).click()
    await driver.sleep(1000)

    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(1)', USERNAME_PROJECT_OPERATION)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(2)', PASSWORD_PROJECT_OPERATION)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(3)', PASSWORD_PROJECT_OPERATION)

    await driver.findElement(By.css('.user-edit-modal .el-dialog__footer:nth-child(3) .el-button--primary')).click()

    await driver.sleep(5000)

    // todo 验证存在这个用户
  })

  // 设置创建项目 query 用户
  it('设置创建项目 query 用户 it_p_q', async () => {
    await driver.findElement(By.css('.security-user .el-row .el-button--primary')).click()
    await driver.sleep(1000)

    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(1)', USERNAME_PROJECT_QUERY)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(2)', PASSWORD_PROJECT_QUERY)
    changeFormInput(driver, '.user-edit-modal .el-form-item:nth-child(3)', PASSWORD_PROJECT_QUERY)

    await driver.findElement(By.css('.user-edit-modal .el-dialog__footer:nth-child(3) .el-button--primary')).click()

    await driver.sleep(5000)

    // todo 验证存在这个用户
  })

  // 进入项目列表页面，找到自动化测试创建的那个项目
  it('进入项目列表页面，找到自动化测试创建的那个项目', async () => {
    await driver.findElement(By.css('#menu-list .el-menu-item:nth-child(1)')).click()
    await driver.sleep(2000)

    changeFormInput(driver, '#project-list .show-search-btn', PROJECT_NAME)
    // todo 执行回车
  })

  // 将这四个用户分配给项目 PROJECT_NAME
  it('将四个用户分配给项目 PROJECT_NAME', async () => {
    // 点击授权按钮
    await driver.sleep(2000)
    await driver.findElement(By.css('.el-icon-ksd-security')).click()
    await driver.sleep(2000)

    // 点击+用户/用户组
    await driver.findElement(By.css('#projectAuth .ksd-fleft .el-button--primary:nth-child(1)')).click()
    await driver.sleep(2000)

    // 先找加号按钮点击三下，生成出四行
    await driver.findElement(By.css('.author_dialog .user-group-select:nth-child(1) .el-icon-ksd-add_2')).click()
    await driver.findElement(By.css('.author_dialog .user-group-select:nth-child(1) .el-icon-ksd-add_2')).click()
    await driver.findElement(By.css('.author_dialog .user-group-select:nth-child(1) .el-icon-ksd-add_2')).click()
    
    // todo 第一个下拉框的选择

    // todo 第二个下拉框的选择

    // todo 第三个下拉框的选择

    // 最后提交
    await driver.findElement(By.css('.author_dialog .el-dialog__footer:nth-child(3) .el-button--primary')).click()

    await driver.sleep(5000)
  })

  // 建模
  it('在项目 PROJECT_NAME 下建模', async () => {

  })

  // 建索引
  it('在项目 PROJECT_NAME 下的模型 MODEL_NAME 建索引', async () => {

  })

  // 查询
  it('查询一条 sql', async () => {

  })
  */
})
