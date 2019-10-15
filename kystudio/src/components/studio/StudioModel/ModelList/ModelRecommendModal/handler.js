import { NamedRegex } from '../../../../../config'
import { objectClone } from 'util/index'

export function checkNameRegex (alias) {
  if (!NamedRegex.test(alias)) {
    return false
  }
  return true
}

export function checkHasSameNamedColumn (list) {
}

// 表渲染已选择的行
export function renderTableColumnSelected (table, that, dom) {
  that.$nextTick(() => {
    if (that.$refs[dom]) {
      table.forEach((col) => {
        that.$refs[dom].toggleRowSelection(col, !!col.isSelected)
      })
    }
  })
}

// 筛选建议类型
export function filterRecommendType (that) {
  if (that.checkedStatus.length === 0) {
    that.tableShowList = objectClone(this.list)
  } else {
    that.tableShowList = that.list.filter((item) => {
      return that.checkedStatus.indexOf(item.recommendation_type) > -1
    })
  }
  renderTableColumnSelected(that.tableShowList)
}
