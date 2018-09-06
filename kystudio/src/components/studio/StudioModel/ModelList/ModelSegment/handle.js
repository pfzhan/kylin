export function formatSegments (segments) {
  return segments.map(segment => {
    return {
      ...segment,
      dateRangeStart: segment.segRange.date_range_start,
      dateRangeEnd: segment.segRange.date_range_end,
      isSelected: false
    }
  })
}
