export const fieldVisiableMaps = {
  'new': [],
  'setting': []
}

export const titleMaps = {
  'new': 'new',
  'setting': 'setting'
}

export function getSubmitData (that) {
  const { editType } = that

  switch (editType) {
    case 'new':
      return {
      }
    case 'setting':
      return {
      }
  }
}
