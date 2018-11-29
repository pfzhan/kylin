export function formatSegments (segments) {
  return segments.map(segment => {
    return {
      ...segment,
      startTime: segment.segRange.date_range_start,
      endTime: segment.segRange.date_range_end !== 9223372036854776000
        ? segment.segRange.date_range_end
        : 7258089600000
    }
  })
}
