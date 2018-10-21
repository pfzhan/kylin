function getSegements () {
  const segments = {}
  const isOnline = Math.random() > 0.3
  const startTime = getRandomDate().getTime()
  const endTime = getRandomDate().getTime()
  for (let i = 0; i < 1000; i++) {
    segments[`TimePartitionedSegmentRange[${startTime},${endTime})`] = isOnline ? 'READY' : 'NEW'
  }
  return segments
}

function getRandomDate () {
  const startYear = `201${Math.ceil(Math.random() * 8 + 1)}`
  const startMonth = Math.ceil(Math.random() * 10 + 1)
  const startDate = Math.ceil(Math.random() * 20 + 1)
  return new Date(`Sun ${startMonth} ${startDate} ${startYear} 14:40:44 GMT+0800 (中国标准时间)`)
}

function getMockModels () {
  const models = []
  for (let i = 0; i < 20; i++) {
    const id = Math.random() * 100000000000000
    models.push({
      'uuid': `89af4ee2-2cdb-4b07-b39e-4c29856309aa${id}`,
      'last_modified': 1536889801000,
      'version': '3.0.0.0',
      'name': `ModelName_${id}`,
      'alias': `ModelName_${id}`,
      'owner': 'ADMIN',
      'is_draft': false,
      'description': null,
      'fact_table': 'DEFAULT.TEST_KYLIN_FACT',
      'management_type': 'TABLE_ORIENTED',
      'metrics': [],
      'filter_condition': null,
      'capacity': 'MEDIUM',
      'auto_merge': true,
      'auto_merge_time_ranges': ['WEEK', 'MONTH'],
      'multilevel_partition_cols': [],
      'computed_columns': [],
      'status': null,
      'segment_ranges': getSegements()
    })
  }
  return models
}

export function getMockModelResponse () {
  return {
    'models': getMockModels(),
    'size': 9999
  }
}
