import dayjs from 'dayjs'

const baseDate = new Date(`Mon Jan 01 2017 06:00:00 GMT+0800 (中国标准时间)`)

let date = dayjs(baseDate)

export function getMockSegments (isReset) {
  const mockSegments = []
  if (isReset) {
    date = dayjs(baseDate)
  }

  for (let i = 0; i < 50; i++) {
    const isZero = Math.random() < 0.2
    const startDate = date.add(i, 'day').valueOf()
    const endDate = date.add(i + 1, 'day').valueOf()

    mockSegments.push({
      id: i,
      uuid: `${Math.floor(Math.random() * 10000000000000000000000000)}`,
      name: `20120111164354_20130109174429${i}`,
      create_time_utc: 1509805113772,
      status: 'READY',
      segRange: {
        '@class': 'org.apache.kylin.metadata.model.SegmentRange$TimePartitionedSegmentRange',
        date_range_start: startDate,
        date_range_end: endDate
      },
      timeRange: null,
      dictionaries: null,
      snapshots: null,
      last_build_time: 0,
      source_count: -1,
      additionalInfo: {},
      size_kb: 36,
      hit_count: !isZero ? Math.random() * 100 : 0
    })
  }

  date = date.add(50, 'day')
  return mockSegments
}
