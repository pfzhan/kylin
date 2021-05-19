/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.parser;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.streaming.metadata.StreamingMessageRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * By convention:
 * 1. stream parsers should have constructor
 *     - with (List<TblColRef> allColumns, Map properties) as params
 *
 */
public abstract class StreamingParser implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(StreamingParser.class);
    public static final String PROPERTY_TS_COLUMN_NAME = "tsColName";
    public static final String PROPERTY_TS_PARSER = "tsParser";
    public static final String PROPERTY_TS_PATTERN = "tsPattern";
    public static final String PROPERTY_EMBEDDED_SEPARATOR = "separator";
    public static final String PROPERTY_STRICT_CHECK = "strictCheck"; // whether need check each column strictly, default be false (fault tolerant).
    public static final String PROPERTY_TS_TIMEZONE = "tsTimezone";

    protected static final Map<String, String> defaultProperties = Maps.newHashMap();

    protected List<TblColRef> allColumns = Lists.newArrayList();
    protected Map<String, String> columnMapping = Maps.newHashMap();

    static {
        defaultProperties.put(PROPERTY_TS_COLUMN_NAME, "timestamp");
        defaultProperties.put(PROPERTY_TS_PARSER, "io.kyligence.kap.parser.DefaultTimeParser");
        defaultProperties.put(PROPERTY_TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        defaultProperties.put(PROPERTY_EMBEDDED_SEPARATOR, "_");
        defaultProperties.put(PROPERTY_STRICT_CHECK, "false");
        defaultProperties.put(PROPERTY_TS_TIMEZONE, "GMT+0");
    }

    /**
     * Constructor should be override
     */
    public StreamingParser(List<TblColRef> allColumns, Map<String, String> properties) {
        if (allColumns != null) {
            initColumns(allColumns);
        }
    }

    private void initColumns(List<TblColRef> allColumns) {
        for (TblColRef col : allColumns) {
            if (col.getColumnDesc().isComputedColumn()) {
                continue;
            }
            this.allColumns.add(col);
        }
        logger.info("Streaming Parser columns: {}", this.allColumns);
    }

    /**
     * Flatten stream message: Extract all key value pairs to a map
     *    The key is source_attribute
     *    The value is the value string
     * @param message
     * @return a flat map must not be NULL
     */
    public abstract Map<String, Object> flattenMessage(ByteBuffer message);

    /**
     * @param message
     * @return List<StreamingMessageRow> must not be NULL
     */
    public abstract List<StreamingMessageRow> parse(ByteBuffer message);

    /**
     * Set column mapping
     * The column mapping: column_name --> source_attribute
     * @param columnMapping
     */
    public void setColumnMapping(Map<String, String> columnMapping) {
        this.columnMapping = columnMapping;
    }

    public static StreamingParser getStreamingParser(String parserName, String parserProperties,
            List<TblColRef> columns) throws ReflectiveOperationException {
        if (!StringUtils.isEmpty(parserName)) {
            logger.info("Construct StreamingParse {} with properties {}", parserName, parserProperties);
            Class clazz = Class.forName(parserName);
            Map<String, String> properties = parseProperties(parserProperties);
            Constructor constructor = clazz.getConstructor(List.class, Map.class);
            return (StreamingParser) constructor.newInstance(columns, properties);
        } else {
            throw new IllegalStateException("Invalid StreamingConfig, parserName " + parserName + ", parserProperties "
                    + parserProperties + ".");
        }
    }

    public static Map<String, String> parseProperties(String propertiesStr) {

        Map<String, String> result = Maps.newHashMap(defaultProperties);
        if (!StringUtils.isEmpty(propertiesStr)) {
            String[] properties = propertiesStr.split(";");
            for (String prop : properties) {
                String[] parts = prop.split("=");
                if (parts.length == 2) {
                    result.put(parts[0], parts[1]);
                } else {
                    logger.warn("Ignored invalid property expression '{}'.", prop);
                }
            }
        }

        return result;
    }

}
