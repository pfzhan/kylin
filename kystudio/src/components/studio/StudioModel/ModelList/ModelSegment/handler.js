import { transToGmtTime } from 'util/business'
export function formatSegments (that, segments) {
  return segments.map(segment => {
    const isFullLoad = segment.segRange.date_range_start === 0 && segment.segRange.date_range_end === 9223372036854776000
    return {
      ...segment,
      status: segment.status_to_display,
      segmentPath: segment.additionalInfo && segment.additionalInfo.segment_path,
      fileNumber: segment.additionalInfo && segment.additionalInfo.file_count,
      startTime: isFullLoad ? that.$t('fullLoad') : transToGmtTime(segment.segRange.date_range_start),
      endTime: isFullLoad ? that.$t('fullLoad') : transToGmtTime(segment.segRange.date_range_end)
    }
  })
}
