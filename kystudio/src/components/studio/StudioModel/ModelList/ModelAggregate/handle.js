import emptyCuboidsUrl from './empty-cuboids.png'

export const backgroundMaps = {
  EMPTY: emptyCuboidsUrl,
  BROKEN: emptyCuboidsUrl
}

export function formatFlowerJson (data) {
  let flowers = []
  let rootLevel = 0
  let maxLevel = 0

  data.forEach(roots => {
    // 获取树的最大level
    Object.values(roots.nodes).forEach(node => {
      node.level > maxLevel && (maxLevel = node.level)
    })
    // 把根节点push进flowers数组
    roots.roots.forEach(root => {
      root = getFlowerData(root, roots.nodes, maxLevel)
      flowers.push(root)
    })

    if (maxLevel > rootLevel) {
      rootLevel = maxLevel
    }
  })

  return [{
    name: 'root',
    id: 'root',
    size: (rootLevel + 1) ** 2 * 200 + 2500,
    children: flowers
  }]
}

export function getCuboidCounts (data) {
  let count = 0
  data.forEach(item => {
    count += Object.keys(item.nodes).length
  })
  return count
}

function getFlowerData (cuboid, nodes = {}, maxLevel) {
  const isRoot = !~cuboid.parent
  if (isRoot) {
    cuboid.name = cuboid.cuboid.id
    cuboid.size = cuboid.cuboid.storage_size || 1
    cuboid.maxLevel = maxLevel
    cuboid.background = cuboid.cuboid.status
  }
  cuboid.children = cuboid.children.map((childId, index) => {
    const child = JSON.parse(JSON.stringify(nodes[childId]))
    child.name = child.cuboid.id
    child.size = child.cuboid.storage_size || 1
    child.background = child.cuboid.status
    child.maxLevel = maxLevel
    if (child.children && child.children.length) {
      getFlowerData(child, nodes, maxLevel)
    }
    return child
  })

  return cuboid
}
