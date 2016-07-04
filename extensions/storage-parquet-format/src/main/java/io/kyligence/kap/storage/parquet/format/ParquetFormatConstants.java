package io.kyligence.kap.storage.parquet.format;

import com.google.common.primitives.Longs;

public interface ParquetFormatConstants {
    String KYLIN_CUBE_ID = "io.kylin.job.cubeid";
    String KYLIN_SEGMENT_ID = "io.kylin.job.segmentid";
    String KYLIN_OUTPUT_DIR = "io.kylin.job.output.path";

    String KYLIN_SCAN_PROPERTIES = "io.kylin.job.properties";
    String KYLIN_SCAN_REQUEST_BYTES = "io.kylin.job.scan.request.bytes";
    String KYLIN_FILTER_MEASURES_BITSET_MAP = "io.kylin.job.filter.measures.bitset";
    String KYLIN_GT_MAX_LENGTH = "io.kylin.job.gtrecord.length.max";

    int KYLIN_PARQUET_TARBALL_HEADER_SIZE = Longs.BYTES;
}
