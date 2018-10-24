export function formatSegments (segments) {
  return segments.map(segment => {
    return {
      ...segment,
      startTime: segment.segRange.date_range_start,
      endTime: segment.segRange.date_range_end
    }
  })
}
