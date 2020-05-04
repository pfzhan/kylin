/* eslint-disable no-extend-native */
if (!String.prototype.trimLeft) {
  String.prototype.trimLeft = function () {
    return this.trim(1)
  }
}

if (!String.prototype.trimRight) {
  String.prototype.trimRight = function () {
    return this.trim(2)
  }
}
