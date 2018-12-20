export function formatSegments (that, segments) {
  return segments.map(segment => {
    const isFullLoad = segment.segRange.date_range_start === 0 && segment.segRange.date_range_end === 9223372036854776000
    return {
      ...segment,
      startTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.date_range_start,
      endTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.date_range_end
    }
  })
}
