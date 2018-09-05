export const USERNAME = 'username'
export const PASSWORD = 'password'
export const CONFIRM_PASSWORD = 'confirm-password'
export const GROUP_NAME = 'group-name'
export const PROJECT_NAME = 'project-name'

// TODO: 在this中解构$t，会造成$t方法中的this为undefined
export default {
  [GROUP_NAME] (rule, value, callback) {
    if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  },

  [USERNAME] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('usernameEmpty')))
    } else {
      callback()
    }
  },

  [PASSWORD] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('passwordEmpty')))
    } else if (value.length < 8) {
      callback(new Error(this.$t('passwordLength')))
    } else if (!/^(?=.*\d)(?=.*[a-z])(?=.*[~!@#$%^&*(){}|:"<>?[\];',./`]).{8,}$/gi.test(value)) {
      callback(new Error(this.$t('kylinLang.user.tip_password_unsafe')))
    } else {
      callback()
    }
  },

  [CONFIRM_PASSWORD] (rule, value, callback) {
    const isPasswordInvalid = this.form.password && value !== this.form.password
    const isNewPasswordInvalid = this.form.newPassword && value !== this.form.newPassword

    if (!value) {
      callback(new Error(this.$t('passwordEmpty')))
    } else if (isPasswordInvalid || isNewPasswordInvalid) {
      callback(new Error(this.$t('passwordConfirm')))
    } else {
      callback()
    }
  },

  [PROJECT_NAME] (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('noProject')))
    } else if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
}
