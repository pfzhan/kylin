export const fieldVisiableMaps = {
  'new': ['username', 'password', 'confirmPassword', 'admin'],
  'password': ['username', 'oldPassword', 'newPassword', 'confirmPassword'],
  'edit': ['username', 'admin'],
  'group': ['group']
}

export const titleMaps = {
  'new': 'addUser',
  'password': 'resetPassword',
  'edit': 'editRole',
  'group': 'kylinLang.common.group'
}

export function getSubmitData (that) {
  const { editType, form, $route } = that

  switch (editType) {
    case 'new':
      return {
        name: form.username,
        detail: {
          username: form.username,
          password: form.password,
          disabled: form.disabled,
          authorities: ($route.params.groupName && !form.authorities.includes($route.params.groupName))
            ? [...form.authorities, $route.params.groupName]
            : form.authorities
        }
      }
    case 'password':
      return {
        username: form.username,
        password: form.oldPassword,
        new_password: form.newPassword
      }
    case 'edit':
      return {
        uuid: form.uuid,
        username: form.username,
        defaultPassword: form.defaultPassword,
        disabled: form.disabled,
        authorities: form.authorities
      }
    case 'group':
      return {
        uuid: form.uuid,
        username: form.username,
        authorities: form.authorities
      }
  }
}
