package io.kyligence.kap.storage.parquet.format;

public interface ParquetFormatConstants {
    String KYLIN_CUBE_ID = "io.kylin.job.cubeid";
    String KYLIN_SEGMENT_ID = "io.kylin.job.segmentid";
    String KYLIN_OUTPUT_DIR = "io.kylin.job.output.path";

    String KYLIN_FILTER_PAGE_BITSET_MAP = "io.kylin.job.filter.page.bitset";
    String KYLIN_FILTER_MEASURES_BITSET_MAP = "io.kylin.job.filter.measures.bitset";
    String KYLIN_GT_MAX_LENGTH = "io.kylin.job.gtrecord.length.max";
    String KYLIN_SCAN_REQUEST = "io.kylin.job.scan.request";
}
