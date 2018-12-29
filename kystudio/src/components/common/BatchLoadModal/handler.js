import dayjs from 'dayjs'
import { getGmtDateFromUtcLike } from '../../../util'

export function getTableLoadRange (table, isSelected = false) {
  const startDate = getGmtDateFromUtcLike(table.startTime)
  const endDate = getGmtDateFromUtcLike(table.endTime)
  const startDateString = dayjs(startDate).format('YYYY-MM-DD')
  const endDateString = dayjs(endDate).format('YYYY-MM-DD')
  return {
    uuid: table.uuid,
    name: table.name,
    fullName: table.fullName,
    refresh: 'Data',
    loadRangeStr: [ startDateString, endDateString ],
    loadRange: [ startDate, endDate ],
    isSelected,
    relatedIndex: 919894
  }
}
