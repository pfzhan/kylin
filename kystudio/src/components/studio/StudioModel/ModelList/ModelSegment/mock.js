const mockSegments = {
  size: 3,
  total_storage_size_kb: 137,
  segments: []
}

for (let i = 0; i < 10000; i++) {
  const isZero = Math.random() < 0.2

  const baseDate = new Date(`Mon Jan 01 2017 00:00:00 GMT+0800 (中国标准时间)`)
  const startDate = baseDate.setDate(baseDate.getDate() + i)
  const endDate = baseDate.setDate(baseDate.getDate() + 1)

  mockSegments.segments.push({
    id: i,
    uuid: `6be0d737-1dc7-41d8-ab8d-a1bd1689307c${i}`,
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

export default mockSegments
