exports.clearFormInput = async function clearFormInput (driver, selector) {
  await driver.executeScript(`
    var input = document.querySelector("${selector}")
    input.value = ""
    // dispatchEvent触发的是原生的event，不是react event。此处有待出解决方案。
    // input.dispatchEvent(new Event("change"))
  `)
}
