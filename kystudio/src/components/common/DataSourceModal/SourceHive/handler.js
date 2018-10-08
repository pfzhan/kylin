export function getDatabaseTree (databases) {
  return databases.map(databaseName => ({
    id: databaseName,
    label: databaseName,
    children: [],
    type: 'database',
    isMore: false,
    isLoading: true,
    pagination: {
      pageOffset: 0,
      pageSize: 10
    }
  }))
}

export function getTableTree (database, res, isTableReset) {
  const { pageSize } = database.pagination
  const newTables = res.tables.map(table => ({
    id: `${database.id}.${table}`,
    label: table,
    type: 'table',
    isSelected: false
  }))
  database.children = isTableReset
    ? newTables
    : database.children.concat(newTables)
  database.isMore = res.size === pageSize
}
