export const fieldVisiableMaps = {
  'new': ['group_name'],
  'assign': ['users']
}

export const titleMaps = {
  'new': 'createGroup',
  'assign': 'kylinLang.common.user'
}

export function getSubmitData (that) {
  const { editType, form } = that

  switch (editType) {
    case 'new':
      return {
        group_name: form.group_name
      }
    case 'assign':
      return {
        group: form.group_name,
        users: form.selected_users
      }
  }
}
