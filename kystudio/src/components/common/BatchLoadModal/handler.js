import { getGmtDateFromUtcLike, transToUTCMs } from '../../../util'

export function getTableLoadRange (table, isSelected = false) {
  const startDate = getGmtDateFromUtcLike(+table.start_time)
  const endDate = getGmtDateFromUtcLike(+table.end_time)
  return {
    fullName: table.table,
    refresh: 'Data',
    loadRange: [ startDate, endDate ],
    isSelected,
    relatedIndex: table.related_index_num
  }
}

export function getTableBatchLoadSubmitData (form, selectedTables) {
  const project = form.project
  const tables = selectedTables
  return tables.map(table => ({
    project,
    table: table.fullName,
    start: String(transToUTCMs(table.loadRange[0])),
    end: String(transToUTCMs(table.loadRange[1]))
  }))
}
