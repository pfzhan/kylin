export const fieldVisiableMaps = {
  'new': []
}

export const titleMaps = {
  'new': 'addAggregateGroup'
}

export function getSubmitData (that) {
  const { editType } = that

  switch (editType) {
    case 'new':
      return {
      }
  }
}

export function getPlaintDimensions (array = []) {
  const dimensions = []
  array.forEach(({ items = [] }) => {
    items.forEach(item => {
      dimensions.push(item)
    })
  })
  return dimensions
}

export function findIncludeDimension (includeOptEls, dimensionValueText) {
  for (let i = 0; i < includeOptEls.length; i++) {
    const includeOptEl = includeOptEls[i]
    const includeOptLabel = includeOptEl.querySelector('.el-select__tags-text').innerHTML

    if (includeOptLabel === dimensionValueText) {
      return includeOptEl
    }
  }
}
