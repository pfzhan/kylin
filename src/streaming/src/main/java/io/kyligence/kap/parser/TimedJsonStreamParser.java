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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.kyligence.kap.streaming.metadata.StreamingMessageRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.collect.Lists;

/**
 * An utility class which parses a JSON streaming message to a list of strings (represent a row in table).
 * <p>
 * Each message should have a property whose value represents the message's timestamp, default the column name is "timestamp"
 * but can be customized by StreamingParser#PROPERTY_TS_PARSER.
 * <p>
 * By default it will parse the timestamp col value as Unix time. If the format isn't Unix time, need specify the time parser
 * with property StreamingParser#PROPERTY_TS_PARSER.
 */
public final class TimedJsonStreamParser extends StreamingParser {

    private final ObjectMapper mapper;
    public static final String EMBEDDED_PROPERTY_SEPARATOR = "|";

    private String flattenSep = null;
    private String tsColName = null;
    private AbstractTimeParser streamTimeParser;

    public TimedJsonStreamParser(List<TblColRef> allColumns, Map<String, String> properties) {
        super(allColumns, properties);
        if (properties == null) {
            properties = StreamingParser.defaultProperties;
        }

        initTimestampColumnProperties(properties);
        mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
        mapper.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);

        flattenSep = properties.get(PROPERTY_EMBEDDED_SEPARATOR);
    }

    private void initTimestampColumnProperties(Map<String, String> properties) {
        tsColName = properties.get(PROPERTY_TS_COLUMN_NAME);
        String tsParser = properties.get(PROPERTY_TS_PARSER);

        if (!StringUtils.isEmpty(tsParser)) {
            try {
                Class clazz = Class.forName(tsParser);
                Constructor constructor = clazz.getConstructor(Map.class);
                streamTimeParser = (AbstractTimeParser) constructor.newInstance(properties);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Invalid StreamingConfig, tsParser " + tsParser + ", parserProperties " + properties + ".", e);
            }
        } else {
            throw new IllegalStateException(
                    "Invalid StreamingConfig, tsParser " + tsParser + ", parserProperties " + properties + ".");
        }
    }

    @Override
    public Map<String, Object> flattenMessage(ByteBuffer message) {
        Map<String, Object> flatMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try {
            traverseJsonNode("", mapper.readTree(message.array()), flatMap);
        } catch (Exception e) {
            throw new KylinException(ServerErrorCode.STREAMING_PARSER_ERROR, "Error when flatten message: " + e);
        }
        return flatMap;
    }

    private void traverseJsonNode(String currentPath, JsonNode jsonNode, Map<String, Object> flatmap) {
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
            String pathPrefix = currentPath.isEmpty() ? "" : currentPath + flattenSep;

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                traverseJsonNode(pathPrefix + entry.getKey(), entry.getValue(), flatmap);
            }
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            if (arrayNode.size() == 0) {
                flatmap.put(currentPath, StringUtils.EMPTY);
            }

            for (int i = 0; i < arrayNode.size(); i++) {
                traverseJsonNode(currentPath + "[" + i + "]", arrayNode.get(i), flatmap);
            }
        } else if (jsonNode.isValueNode()) {
            ValueNode valueNode = (ValueNode) jsonNode;
            getJsonValueByType(currentPath, flatmap, valueNode);
        }
    }

    private void getJsonValueByType(String currentPath, Map<String, Object> flatmap, ValueNode valueNode) {
        if (valueNode.isShort())
            addValueToFlatMap(flatmap, currentPath, valueNode.shortValue());
        else if (valueNode.isInt())
            addValueToFlatMap(flatmap, currentPath, valueNode.intValue());
        else if (valueNode.isLong())
            addValueToFlatMap(flatmap, currentPath, valueNode.longValue());
        else if (valueNode.isBigDecimal())
            addValueToFlatMap(flatmap, currentPath, valueNode.decimalValue());
        else if (valueNode.isFloat())
            addValueToFlatMap(flatmap, currentPath, valueNode.floatValue());
        else if (valueNode.isDouble())
            addValueToFlatMap(flatmap, currentPath, valueNode.doubleValue());
        else if (valueNode.isBoolean())
            addValueToFlatMap(flatmap, currentPath, valueNode.booleanValue());
        else
            addValueToFlatMap(flatmap, currentPath, valueNode.asText());
    }

    private void addValueToFlatMap(Map<String, Object> flatmap, String key, Object val) {
        // to avoid key duplicated
        addValueToFlatMap(flatmap, key, val, 0);
    }

    private void addValueToFlatMap(Map<String, Object> flatmap, String key, Object val, int iteTime) {
        if (flatmap.containsKey(key)) {
            key = key + "_" + (++iteTime);
            addValueToFlatMap(flatmap, key, val, iteTime);
        } else {
            flatmap.put(key, val);
        }
    }

    private String getSourceAttributeByColumnName(String columnName) {
        if (columnMapping == null || columnMapping.isEmpty()) {
            return columnName;
        }
        return columnMapping.getOrDefault(columnName, columnName);
    }

    @Override
    public List<StreamingMessageRow> parse(ByteBuffer buffer) {
        try {
            Map<String, Object> map = flattenMessage(buffer);
            String tsColSrcAttribute = getSourceAttributeByColumnName(tsColName);
            String tsStr = getValueFromFlatMap(map, tsColSrcAttribute);
            long ts = streamTimeParser.parseTime(tsStr);
            ArrayList<String> result = Lists.newArrayList();

            for (TblColRef column : allColumns) {
                final String colSrcAttr = getSourceAttributeByColumnName(column.getName());
                if (colSrcAttr.equalsIgnoreCase(tsColSrcAttribute)) {
                    result.add(DateFormat.formatToTimeWithoutMilliStr(ts));
                } else {
                    result.add(getValueFromFlatMap(map, colSrcAttr));
                }
            }

            StreamingMessageRow streamingMessageRow = new StreamingMessageRow(result, 0, ts, Collections.emptyMap());
            List<StreamingMessageRow> messageRowList = new ArrayList<>();
            messageRowList.add(streamingMessageRow);
            return messageRowList;
        } catch (Exception e) {
            throw new KylinException(ServerErrorCode.STREAMING_PARSER_ERROR, "Error when parse message. " + e);
        }
    }

    private String getValueFromFlatMap(Map<String, Object> map, String colName) {
        return map.getOrDefault(colName, StringUtils.EMPTY).toString();
    }

}
