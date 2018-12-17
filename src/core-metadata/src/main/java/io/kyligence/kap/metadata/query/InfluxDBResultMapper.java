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

package io.kyligence.kap.metadata.query;

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBMapperException;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@NoArgsConstructor
public class InfluxDBResultMapper {
    private static final DateTimeFormatter ISO8601_FORMATTER = (new DateTimeFormatterBuilder())
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendPattern("X").toFormatter();
    private static final ConcurrentMap<String, ConcurrentMap<String, Field>> CLASS_FIELD_CACHE = new ConcurrentHashMap();

    public <T> List<T> toPOJO(QueryResult queryResult, Class<T> clazz, String tableName)
            throws InfluxDBMapperException {
        cacheMeasurementClass(clazz);
        List<T> result = new LinkedList();
        queryResult.getResults().stream().filter((internalResult) -> {
            return Objects.nonNull(internalResult) && Objects.nonNull(internalResult.getSeries());
        }).forEach((internalResult) -> {
            internalResult.getSeries().stream().filter((series) -> {
                return series.getName().equals(tableName);
            }).forEachOrdered((series) -> {
                parseSeriesAs(series, clazz, result);
            });
        });
        return result;
    }

    void cacheMeasurementClass(Class clazz) {
        if (!CLASS_FIELD_CACHE.containsKey(clazz.getName())) {
            ConcurrentMap<String, Field> initialMap = new ConcurrentHashMap();
            ConcurrentMap<String, Field> influxColumnAndFieldMap = CLASS_FIELD_CACHE.putIfAbsent(clazz.getName(),
                    initialMap);
            if (influxColumnAndFieldMap == null) {
                influxColumnAndFieldMap = initialMap;
            }

            Field[] fields = clazz.getDeclaredFields();

            for (int i = 0; i < fields.length; ++i) {
                Field field = fields[i];
                Column colAnnotation = field.getAnnotation(Column.class);
                if (colAnnotation != null) {
                    ((ConcurrentMap) influxColumnAndFieldMap).put(colAnnotation.name(), field);
                }
            }
        }
    }

    <T> List<T> parseSeriesAs(QueryResult.Series series, Class<T> clazz, List<T> result) {
        int columnSize = series.getColumns().size();
        ConcurrentMap colNameAndFieldMap = CLASS_FIELD_CACHE.get(clazz.getName());

        try {
            T object = null;
            Iterator nextSeries = series.getValues().iterator();

            while (nextSeries.hasNext()) {
                List<Object> row = (List) nextSeries.next();

                for (int i = 0; i < columnSize; ++i) {
                    Field correspondingField = (Field) colNameAndFieldMap.get(series.getColumns().get(i));
                    if (correspondingField != null) {
                        if (object == null) {
                            object = clazz.newInstance();
                        }

                        setFieldValue(object, correspondingField, row.get(i));
                    }
                }

                findTags(series, colNameAndFieldMap, object);

                if (object != null) {
                    result.add(object);
                    object = null;
                }
            }

            return result;
        } catch (IllegalAccessException | InstantiationException ex) {
            throw new InfluxDBMapperException(ex);
        }
    }

    <T> void findTags(QueryResult.Series series, ConcurrentMap colNameAndFieldMap, T object) {
        if (series.getTags() != null && !series.getTags().isEmpty()) {
            Iterator tag = series.getTags().entrySet().iterator();

            while (tag.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry) tag.next();
                Field correspondingField = (Field) colNameAndFieldMap.get(entry.getKey());
                if (correspondingField != null) {
                    try {
                        setFieldValue(object, correspondingField, entry.getValue());
                    } catch (IllegalAccessException e) {
                        throw new InfluxDBMapperException(e);
                    }
                }
            }
        }
    }

    <T> void setFieldValue(T object, Field field, Object value)
            throws IllegalArgumentException, IllegalAccessException {
        if (value != null) {
            Class fieldType = field.getType();

            try {
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }

                if (!fieldValueModified(fieldType, field, object, value)
                        && !fieldValueForPrimitivesModified(fieldType, field, object, value)
                        && !fieldValueForPrimitiveWrappersModified(fieldType, field, object, value)) {
                    String msg = "Class '%s' field '%s' is from an unsupported type '%s'.";
                    throw new InfluxDBMapperException(
                            String.format(msg, object.getClass().getName(), field.getName(), field.getType()));
                }
            } catch (ClassCastException var7) {
                String msg = "Class '%s' field '%s' was defined with a different field type and caused a ClassCastException. The correct type is '%s' (current field value: '%s').";
                throw new InfluxDBMapperException(String.format(msg, object.getClass().getName(), field.getName(),
                        value.getClass().getName(), value));
            }
        }
    }

    <T> boolean fieldValueModified(Class<?> fieldType, Field field, T object, Object value)
            throws IllegalArgumentException, IllegalAccessException {
        if (String.class.isAssignableFrom(fieldType)) {
            field.set(object, String.valueOf(value));
            return true;
        } else if (Instant.class.isAssignableFrom(fieldType)) {
            Instant instant;
            if (value instanceof String) {
                instant = Instant.from(ISO8601_FORMATTER.parse(String.valueOf(value)));
            } else if (value instanceof Long) {
                instant = Instant.ofEpochMilli((Long) value);
            } else {
                if (!(value instanceof Double)) {
                    throw new InfluxDBMapperException(
                            "Unsupported type " + field.getClass() + " for field " + field.getName());
                }

                instant = Instant.ofEpochMilli(((Double) value).longValue());
            }

            field.set(object, instant);
            return true;
        } else {
            return false;
        }
    }

    <T> boolean fieldValueForPrimitivesModified(Class<?> fieldType, Field field, T object, Object value)
            throws IllegalArgumentException, IllegalAccessException {
        if (Double.TYPE.isAssignableFrom(fieldType)) {
            field.setDouble(object, (Double) value);
            return true;
        } else if (Long.TYPE.isAssignableFrom(fieldType)) {
            field.setLong(object, ((Double) value).longValue());
            return true;
        } else if (Integer.TYPE.isAssignableFrom(fieldType)) {
            field.setInt(object, ((Double) value).intValue());
            return true;
        } else if (Boolean.TYPE.isAssignableFrom(fieldType)) {
            field.setBoolean(object, Boolean.valueOf(String.valueOf(value)));
            return true;
        } else {
            return false;
        }
    }

    <T> boolean fieldValueForPrimitiveWrappersModified(Class<?> fieldType, Field field, T object, Object value)
            throws IllegalArgumentException, IllegalAccessException {
        if (Double.class.isAssignableFrom(fieldType)) {
            field.set(object, value);
            return true;
        } else if (Long.class.isAssignableFrom(fieldType)) {
            field.set(object, ((Double) value).longValue());
            return true;
        } else if (Integer.class.isAssignableFrom(fieldType)) {
            field.set(object, ((Double) value).intValue());
            return true;
        } else if (Boolean.class.isAssignableFrom(fieldType)) {
            field.set(object, Boolean.valueOf(String.valueOf(value)));
            return true;
        } else {
            return false;
        }
    }
}
