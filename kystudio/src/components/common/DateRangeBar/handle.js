function getGroupTypeObj (groupTypeArray = [], during) {
  for (let i = 0; i < groupTypeArray.length; i++) {
    const [groupType, groupMax] = groupTypeArray[i]
    const groupMin = groupTypeArray[i - 1] && groupTypeArray[i - 1][1] || 0
    const isInGroupType = groupMin <= during && during <= groupMax
    const isOutGroupMax = i === groupTypeArray.length - 1 && during > groupMax

    if (isInGroupType || isOutGroupMax) {
      return groupType
    }
  }
}

export function handleAutoGroup (dataArray = [], groupMap = []) {
  const groupTypes = Object.entries(groupMap)

  let newDataArray = [...dataArray]
  let duringAcc = 0
  let minGroupType = null
  let minGroupTypeId = null
  for (const item of newDataArray) {
    // 获取每个数据当前分组类型
    const groupType = getGroupTypeObj(groupTypes, item.endDate - item.startDate)
    item.groupType = groupType
    // 找出最小groupType
    if (!minGroupTypeId || groupMap[groupType] < minGroupTypeId) {
      minGroupTypeId = groupMap[groupType]
      minGroupType = groupType
    }
  }

  // 对每种类型进行分组
  for (const [groupType, groupMax] of groupTypes) {
    // 性能优化，比最小groupType相等或还小的groupType，不去遍历
    if (groupMax <= groupMap[minGroupType]) continue
    // 遍历分组
    const groupArray = newDataArray.reduce((result, item, i) => {
      const lastItem = newDataArray[i - 1]
      const isItemDifferent = lastItem && (
        lastItem.color !== item.color ||
        lastItem.groupType !== item.groupType)

      duringAcc += item.endDate - item.startDate

      if (!result.length || isItemDifferent || duringAcc > groupMax) {
        result.push([item])
        duringAcc = item.endDate - item.startDate
      } else {
        result[result.length - 1].push(item)
      }

      return result
    }, [])

    newDataArray = []

    groupArray.forEach(group => {
      const firstItem = group[0]
      const lastItem = group[group.length - 1]
      const isMerge = lastItem && lastItem.endDate - firstItem.startDate >= groupMax
      if (!isMerge) {
        newDataArray = [...newDataArray, ...group]
      } else {
        const firstItem = group[0]
        const lastItem = group[group.length - 1]
        newDataArray = [...newDataArray, { ...firstItem, groupType, endDate: lastItem.endDate }]
      }
    })
  }

  return newDataArray
}

export function handleCalcRangeSize (ranges, groupMap, totalWidth) {
  const firstRange = ranges[0]
  const lastRange = ranges[ranges.length - 1]
  const total = lastRange.endDate - firstRange.startDate

  let lastX = 0
  for (const range of ranges) {
    range.width = groupMap[range.groupType] / total * totalWidth
    range.left = lastX
    lastX += range.width
  }
  return ranges
}
