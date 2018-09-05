const mockSegments = {
  size: 3,
  total_storage_size_kb: 137,
  segments: []
}

for (let i = 1; i < 100; i++) {
  const isZero = Math.random() < 0.2

  const baseDate = new Date(`Mon Jan 01 2017 00:00:00 GMT+0800 (中国标准时间)`)
  const startDate = baseDate.setDate(baseDate.getDate() + i)
  const endDate = baseDate.setDate(baseDate.getDate() + 1)

  mockSegments.segments.push({
    size_kb: 36,
    snapshots: null,
    source_offset_end: 0,
    source_offset_start: 0,
    status: 'READY',
    storage_location_identifier: 'KYLIN_HE2YMKK60C',
    total_shards: 0,
    uuid: `6be0d737-1dc7-41d8-ab8d-a1bd1689307c${i}`,
    name: '20120111164354_20130109174429',
    last_build_time: 1532167086872,
    date_range_start: startDate,
    date_range_end: endDate,
    hit_count: !isZero ? Math.random() * 100 : 0
  })
}

export default mockSegments
