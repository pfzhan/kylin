export function _getPartitionInfo (project, table, partitionColumn) {
  return {
    project: project.name,
    table: `${table.database}.${table.name}`,
    column: partitionColumn
  }
}

export function _getFullLoadInfo (project, table) {
  return {
    project: project.name,
    table: `${table.database}.${table.name}`
  }
}
