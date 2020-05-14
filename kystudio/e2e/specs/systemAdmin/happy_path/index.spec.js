const { Builder, By, until } = require('selenium-webdriver')
const assert = require('assert')
const { login, logout, addUser } = require('../../utils/businessHelper')
const { changeFormInput, changeFormTextarea, hoverOn, changeFormSelect } = require('../../utils/domHelper')

const {
  BROWSER_ENV,
  LAUNCH_URL,
  USERNAME_ADMIN,
  PASSWORD_ADMIN,
  PASSWORD_ADMIN_NEW,
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
  this.timeout(60000)
  let driver

  before(async () => {
    driver = await new Builder().forBrowser(BROWSER_ENV).build()
  })

  after(async () => {
    await driver.quit()
  })

  // 修改默认密码
  it('修改默认密码', async () => {
    await driver.get(LAUNCH_URL)
    await driver.manage().window().setRect(1440, 828)

    // 统一调用登录
    await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN);
    await driver.sleep(3000)

    try {
      await driver.wait(until.elementIsVisible(driver.findElement(By.css('.user-edit-modal'))), 10000)
    } catch (e){
      return true
    }
    // 修改密码的弹窗
    changeFormInput(driver, '.user-edit-modal .el-dialog__body .js_oldPassword', PASSWORD_ADMIN)
    changeFormInput(driver, '.user-edit-modal .el-dialog__body .js_newPassword', PASSWORD_ADMIN_NEW)
    changeFormInput(driver, '.user-edit-modal .el-dialog__body .js_confirmPwd', PASSWORD_ADMIN_NEW)
    await driver.sleep(1000)
    await driver.findElement(By.css('.user-edit-modal .el-dialog__footer .el-button--primary')).click()
    await driver.sleep(4000)
  })

  it('登出，让修改的密码生效', async () => {
    // 执行登出，让修改后的账密状态更新
    await logout(driver)
    await driver.sleep(2000)
  })

  // 创建项目
  it('创建项目', async () => {
    // 统一调用登录
    await login(driver, USERNAME_ADMIN, PASSWORD_ADMIN_NEW);

    await driver.wait(until.elementLocated(By.css('.topbar .add-project-btn')), 3000)
    await driver.findElement(By.css('.topbar .add-project-btn')).click()

    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.project-edit-modal'))), 10000)
    changeFormInput(driver, '.project-edit-modal .js_projectname', PROJECT_NAME)
    changeFormTextarea(driver, '.project-edit-modal .js_project_desc', 'this is it test auto create project')
    await driver.sleep(1000)
    await driver.findElement(By.css('.project-edit-modal .el-dialog__footer .js_addproject_submit')).click()

    // 接口有时候是 2 秒等待
    await driver.sleep(3000)
    // 添加完成后，进入的是数据源页面
    assert.equal(await driver.getCurrentUrl(), `${LAUNCH_URL}/#/studio/source`);
  })
  
  // 加载数据源
  it('加载数据源', async () => {
    await driver.findElement(By.css('.data-source-bar .btn-group .el-button--primary')).click()
    // until 弹窗出来后 点击 hive
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('.data-srouce-modal'))), 10000)
    await driver.findElement(By.css('.data-srouce-modal .source-new .datasouce .el-icon-ksd-hive')).click()
    await driver.sleep(1000)
    
    // 点击下一步按钮
    await driver.findElement(By.css('.data-srouce-modal .el-dialog__footer .el-button--primary')).click()
    
    // 找到 ssb，点击选中所有
    await hoverOn(driver, By.css('.guide-ssb'))
    await driver.findElement(By.css('.guide-ssb .select-all')).click()
    await driver.sleep(1000)
    await driver.findElement(By.css('.data-srouce-modal .el-dialog__footer .el-button--primary')).click()
    await driver.sleep(3000)
  })

  // 设置默认数据库
  it('配置默认数据库', async () => {
    // 点击菜单设置
    await driver.findElement(By.css('.el-menu-item .el-icon-ksd-setting')).click()
    
    // 点击高级设置的 tab
    await driver.wait(until.elementLocated(By.id('tab-advanceSetting')), 3000)
    await driver.findElement(By.id('tab-advanceSetting')).click()
    // 选择 下拉里的 ssb
    await driver.wait(until.elementLocated(By.css('.js_defautDB_block')), 3000)
    await changeFormSelect(driver, '.js_defautDB_block .js_select', '.js_defautDB_select .el-select-dropdown__list', 2)
    await driver.sleep(1000)
    // 点击提交
    await driver.findElement(By.css('#pane-advanceSetting .js_defautDB_block .block-foot .el-button--default')).click()
    // 点击二次确认弹窗的按钮
    await driver.wait(until.elementIsVisible(driver.findElement(By.css('div[aria-label=修改默认数据库]'))), 10000)
    await driver.findElement(By.css('div[aria-label=修改默认数据库] .el-message-box__btns .el-button--primary')).click()
    await driver.sleep(3000)
  })

  // 进入系统管理页面
  it('进入系统管理页面', async () => {
    await driver.findElement(By.css('.entry-admin')).click()
    // 点击菜单 用户
    await driver.wait(until.elementLocated(By.css('.el-menu-item .el-icon-ksd-table_admin')), 10000)
    await driver.findElement(By.css('.el-menu-item .el-icon-ksd-table_admin')).click()
    await driver.sleep(2000)
  })

  
  // 设置创建项目 admin 用户
  it('设置创建项目 admin 用户 it_p_admin', async () => {
    await addUser(driver, USERNAME_PROJECT_ADMIN, PASSWORD_PROJECT_ADMIN)
  })

  // 设置创建项目 management 用户
  it('设置创建项目 management 用户 it_p_m', async () => {
    await addUser(driver, USERNAME_PROJECT_MANAGEMENT, PASSWORD_PROJECT_MANAGEMENT)
  })
  
  // 设置创建项目 operator 用户
  it('设置创建项目 operator 用户 it_p_o', async () => {
    await addUser(driver, USERNAME_PROJECT_OPERATION, PASSWORD_PROJECT_OPERATION)
  })

  // 设置创建项目 query 用户
  it('设置创建项目 query 用户 it_p_q', async () => {
    await addUser(driver, USERNAME_PROJECT_QUERY, PASSWORD_PROJECT_QUERY)
  })

  /*
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
