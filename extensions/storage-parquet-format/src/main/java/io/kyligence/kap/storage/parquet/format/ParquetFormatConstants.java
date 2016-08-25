package io.kyligence.kap.storage.parquet.format;

import com.google.common.primitives.Longs;

public interface ParquetFormatConstants {
    String KYLIN_OUTPUT_DIR = "io.kylin.job.output.path";

    String KYLIN_SCAN_PROPERTIES = "io.kylin.storage.parquet.scan.properties";
    String KYLIN_SCAN_REQUEST_BYTES = "io.kylin.storage.parquet.scan.request";
    String KYLIN_SCAN_REQUIRED_PARQUET_COLUMNS = "io.kylin.storage.parquet.scan.parquetcolumns";
    String KYLIN_GT_MAX_LENGTH = "io.kylin.storage.parquet.scan.gtrecord.maxlength";
    String KYLIN_USE_INVERTED_INDEX = "io.kylin.storage.parquet.scan.useii";
    String KYLIN_TARBALL_READ_STRATEGY = "io.kylin.storage.parquet.read.strategy";

    int KYLIN_PARQUET_TARBALL_HEADER_SIZE = Longs.BYTES;
}
