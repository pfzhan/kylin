export function getPlaintTreeNode (parent) {
  Object.keys(parent.cuboidDesc).forEach(key => {
    parent[key] = parent.cuboidDesc[key]
  })

  if (parent.children.length) {
    for (const child of parent.children) {
      getPlaintTreeNode(child)
    }
  }

  return parent
}
