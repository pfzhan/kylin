export function formatFlowerJson (data) {
  let flowers = []
  let rootLevel = 0

  data.forEach(roots => {
    let maxLevel = 0
    // 获取树的最大level
    Object.values(roots.nodes).forEach(node => {
      node.level > maxLevel && (maxLevel = node.level)
    })
    // 把根节点push进flowers数组
    roots.roots.forEach(root => {
      root = getFlowerData(root, maxLevel, true)
      flowers.push(root)
    })

    if (maxLevel > rootLevel) {
      rootLevel = maxLevel
    }
  })

  if (flowers.length === 1) {
    return flowers
  } else {
    return [{
      name: 'root',
      id: 'root',
      size: (rootLevel + 1) ** 2 * 200 + 2500,
      children: flowers
    }]
  }
}

export function getCuboidCounts (data) {
  let count = 0
  data.forEach(item => {
    count += Object.keys(item.nodes).length
  })
  return count
}

function getFlowerData (parent, maxLevel, isRoot) {
  if (isRoot) {
    parent.name = parent.cuboid.id
    parent.size = (maxLevel - parent.level) ** 2 * 200 + 2500
  }
  parent.children = parent.children.map((child) => {
    child.name = child.cuboid.id
    child.size = (maxLevel - child.level) ** 2 * 200 + 2500
    child.children && child.children.length && getFlowerData(child, maxLevel)
    return child
  })

  return parent
}
