const { until, By, Key } = require('selenium-webdriver')
const { waitingForStable, clearFormInput, changeFormInput, changeFormSelect } = require('../utils/domHelper')

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
  // 浏览器会自动填入 admin，所以要先置空用户名的输入框
  await clearFormInput(driver, '.login-form .input_group .el-form-item:nth-child(1) input')
  await driver.sleep(2000)
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

exports.addUser = async function addUser (driver, username, pwd) {
  await driver.sleep(2000)
  await driver.findElement(By.css('.security-user .el-row .el-button--primary')).click()
  await driver.sleep(1000)

  changeFormInput(driver, '.user-edit-modal .js_username', username)
  await driver.sleep(1000)
  changeFormInput(driver, '.user-edit-modal .js_password', pwd)
  await driver.sleep(1000)
  changeFormInput(driver, '.user-edit-modal .js_confirmPwd', pwd)
  await driver.sleep(1000)
  await driver.findElement(By.css('.user-edit-modal .el-dialog__footer .el-button--primary')).click()
  await driver.sleep(5000)
}

exports.delUser = async function delUser (driver, username, idx) {
  // 先清空搜索
  await clearFormInput(driver, '.show-search-btn input')
  await driver.sleep(1000)

  // 精确搜索想要删除的用户，保证列表只有一条记录
  await changeFormInput(driver, '.show-search-btn', username)
  await driver.sleep(1000)

  const actions = driver.actions({bridge: true})
  // 执行回车搜索
  await actions.click(await driver.findElement(By.css('.show-search-btn input'))).sendKeys(Key.ENTER).perform()

  await driver.sleep(2000)
  // 点击右侧更多按钮
  await driver.findElement(By.css('.el-icon-ksd-table_others')).click()
  let moreBtnEl = await driver.findElement(By.css('.el-icon-ksd-table_others'))
  // 更多按钮上的 aria-controls 属性对应的就是 dropdown 的下拉 div 的id
  let dropdownMenuId = await moreBtnEl.getAttribute('aria-controls')
  
  await driver.actions().move({ origin: moreBtnEl }).perform()
  await driver.sleep(1000)

  await driver.wait(until.elementLocated(By.id(dropdownMenuId)), 10000)
  await driver.findElement(By.css(`ul#${dropdownMenuId} .el-dropdown-menu__item:nth-child(${idx})`)).click()

  await driver.wait(until.elementIsVisible(driver.findElement(By.css('div[aria-label=删除用户] .el-message-box__btns .el-button--primary'))), 10000)
  await driver.findElement(By.css('div[aria-label=删除用户] .el-message-box__btns .el-button--primary')).click()
  await driver.sleep(2000)
}

exports.searchCurProject = async function searchCurProject (driver, projectname) {
  await changeFormInput(driver, '#project-list .show-search-btn', projectname)
  await driver.sleep(1000)

  const actions = driver.actions({bridge: true})
  // 执行回车搜索
  await actions.click(await driver.findElement(By.css('#project-list .show-search-btn input'))).sendKeys(Key.ENTER).perform()
  await driver.sleep(2000)
}

exports.setUserToProject = async function setUserToProject (driver, username, lineIdx, typeIdx) {
  // 选 principal
  const userSel = `.author_dialog .user-group-select:nth-child(${lineIdx}) .user-select`
  await changeFormSelect(driver, userSel, '.js_principal', 1)
  await driver.sleep(1000)

  // 选具体的人，需要先搜这个人，然后匹配出下拉第几位
  const user = `.author_dialog .user-group-select:nth-child(${lineIdx}) .name-select`
  await changeFormInput(driver, user, username)
  await driver.sleep(1000)
  let userIdx = 0
  let userList = await driver.findElements('.author-select .el-select-dropdown__item')
  for (let i = 0; i < userList.length; i++) {
    let item = userList[i]
    let text = item.getText()
    if (text === username) {
      userIdx = i
      break
    }
  }
  await changeFormSelect(driver, user, '.author-select', userIdx)
  await driver.sleep(1000)

  // 选具体的权限
  const typeSel = `.author_dialog .user-group-select:nth-child(${lineIdx}) .type-select`
  await changeFormSelect(driver, typeSel, '.js_access_type_sel', typeIdx)
  await driver.sleep(1000)
}