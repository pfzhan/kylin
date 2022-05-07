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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

/**
 * default stream JSON parser
 * recursive parse JSON
 */
public class TimedJsonStreamParser extends AbstractDataParser<ByteBuffer> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected Map<String, Object> parse(ByteBuffer input) {

        Map<String, Object> flatMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try {
            traverseJsonNode("", mapper.readTree(input.array()), flatMap);
        } catch (Exception e) {
            throw new KylinException(ServerErrorCode.STREAMING_PARSER_ERROR, "Error when flatten message: " + e);
        }
        return flatMap;

    }

    private void traverseJsonNode(String currentPath, JsonNode jsonNode, Map<String, Object> flatmap) {
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
            String pathPrefix = currentPath.isEmpty() ? "" : currentPath + "_";

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
                traverseJsonNode(currentPath + "__" + i, arrayNode.get(i), flatmap);
            }
        } else if (jsonNode.isValueNode()) {
            ValueNode valueNode = (ValueNode) jsonNode;
            getJsonValueByType(currentPath, flatmap, valueNode);
        }
    }

    private void getJsonValueByType(String currentPath, Map<String, Object> flatmap, ValueNode valueNode) {
        if (valueNode.isShort()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.shortValue());
        } else if (valueNode.isInt()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.intValue());
        } else if (valueNode.isLong()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.longValue());
        } else if (valueNode.isBigDecimal()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.decimalValue());
        } else if (valueNode.isFloat()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.floatValue());
        } else if (valueNode.isDouble()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.doubleValue());
        } else if (valueNode.isBoolean()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.booleanValue());
        } else {
            addValueToFlatMap(flatmap, currentPath, valueNode.asText());
        }
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
}
