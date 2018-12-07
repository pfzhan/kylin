export const fieldVisiableMaps = {
  'new': ['groupName'],
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
        groupName: form.groupName
      }
    case 'assign':
      return {
        group: form.groupName,
        users: form.selectedUsers
      }
  }
}
