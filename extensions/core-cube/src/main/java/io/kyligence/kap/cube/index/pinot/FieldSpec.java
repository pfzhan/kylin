/**
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

package io.kyligence.kap.cube.index.pinot;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 */
// Json annotation required for abstract classes.
//@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class FieldSpec {
    private static final String DEFAULT_DIM_NULL_VALUE_OF_STRING = "null";
    private static final Integer DEFAULT_DIM_NULL_VALUE_OF_INT = Integer.valueOf(Integer.MIN_VALUE);
    private static final Long DEFAULT_DIM_NULL_VALUE_OF_LONG = Long.valueOf(Long.MIN_VALUE);
    private static final Float DEFAULT_DIM_NULL_VALUE_OF_FLOAT = Float.valueOf(Float.NEGATIVE_INFINITY);
    private static final Double DEFAULT_DIM_NULL_VALUE_OF_DOUBLE = Double.valueOf(Double.NEGATIVE_INFINITY);

    private static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = Integer.valueOf(0);
    private static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = Long.valueOf(0);
    private static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = Float.valueOf(0);
    private static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = Double.valueOf(0);

    String name;
    FieldType fieldType;
    DataType dataType;
    boolean isSingleValueField = true;
    String delimiter = ",";
    Object defaultNullValue;

    public FieldSpec() {

    }

    public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue, String delimeter) {
        this.name = name;
        this.fieldType = fType;
        this.dataType = dType;
        this.isSingleValueField = singleValue;
        this.delimiter = delimeter;
    }

    public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue, String delimeter, Object defaultNullValue) {
        this.name = name;
        fieldType = fType;
        dataType = dType;
        isSingleValueField = singleValue;
        delimiter = delimeter;
        this.defaultNullValue = defaultNullValue;
    }

    public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue) {
        this(name, fType, dType, singleValue, null);
    }

    public FieldSpec(String name, FieldType fType, DataType dType, String delimeter) {
        this(name, fType, dType, false, delimeter);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimeter) {
        delimiter = delimeter;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public boolean isSingleValueField() {
        return isSingleValueField;
    }

    public void setSingleValueField(boolean isSingleValueField) {
        this.isSingleValueField = isSingleValueField;
    }

    // This method is invoked via ObjectMapper, in Schema.java.
    // Do NOT change the signature or remove it.
    public void setDefaultNullValue(Object value) {
        this.defaultNullValue = value;
    }

    // Called when trying to set a default null value from hadoop.
    public void defaultNullValueFromString(String value) {
        Object defaultNullObj;
        try {
            switch (getDataType()) {
            case INT:
            case INT_ARRAY:
                defaultNullObj = new Integer(Integer.valueOf(value));
                break;
            case LONG:
            case LONG_ARRAY:
                defaultNullObj = new Long(Long.valueOf(value));
                break;
            case FLOAT:
            case FLOAT_ARRAY:
                defaultNullObj = new Float(Float.valueOf(value));
                break;
            case DOUBLE:
            case DOUBLE_ARRAY:
                defaultNullObj = new Double(Double.valueOf(value));
                break;
            case STRING:
            case STRING_ARRAY:
                defaultNullObj = value;
                break;
            default:
                throw new UnsupportedOperationException("Default null value not supported for field type " + getDataType());
            }
        } catch (NumberFormatException e) {
            throw new UnsupportedOperationException("Cannot set default null value", e);
        }
        this.defaultNullValue = defaultNullObj;
    }

    @Override
    public String toString() {
        return "< data type : " + dataType + " , field type : " + fieldType + ((isSingleValueField) ? ", single value column" : ", multi value column") + ", delimeter : " + delimiter + " >";
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        return this.toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * FieldType is used to demonstrate the real world business logic for a column.
     *
     */
    public enum FieldType {
        UNKNOWN, DIMENSION, METRIC, TIME
    }

    /**
     * DataType is used to demonstrate the data type of a column.
     *
     */
    public enum DataType {
        BOOLEAN, BYTE, CHAR, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, OBJECT,
        //EVERYTHING AFTER THIS MUST BE ARRAY TYPE
        BYTE_ARRAY, CHAR_ARRAY, SHORT_ARRAY, INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY;

        public boolean isNumber() {
            return (this == BYTE) || (this == SHORT) || (this == INT) || (this == LONG) || (this == FLOAT) || (this == DOUBLE);
        }

        public boolean isInteger() {
            return (this == BYTE) || (this == SHORT) || (this == INT) || (this == LONG);
        }

        public boolean isSingleValue() {
            return this.ordinal() < BYTE_ARRAY.ordinal();
        }

        /**
         * return the number of bytes
         * @return
         */
        public int size() {
            switch (this) {
            case BYTE:
                return 1;
            case SHORT:
                return 2;
            case INT:
                return 4;
            case LONG:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            default:
                throw new UnsupportedOperationException("Cant get number of bytes for :" + this);
            }
        }

    }

    public Object getDefaultNullValue() {
        if (defaultNullValue != null) {
            return defaultNullValue;
        }
        switch (getFieldType()) {
        case METRIC:
            switch (dataType) {
            case INT:
            case INT_ARRAY:
                return DEFAULT_METRIC_NULL_VALUE_OF_INT;
            case LONG:
            case LONG_ARRAY:
                return DEFAULT_METRIC_NULL_VALUE_OF_LONG;
            case FLOAT:
            case FLOAT_ARRAY:
                return DEFAULT_METRIC_NULL_VALUE_OF_FLOAT;
            case DOUBLE:
            case DOUBLE_ARRAY:
                return DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE;
            default:
                throw new UnsupportedOperationException("Unknown default null value for metric of data type " + dataType);
            }
        case DIMENSION:
        case TIME:
            switch (getDataType()) {
            case INT:
            case INT_ARRAY:
                return DEFAULT_DIM_NULL_VALUE_OF_INT;
            case LONG:
            case LONG_ARRAY:
                return DEFAULT_DIM_NULL_VALUE_OF_LONG;
            case FLOAT:
            case FLOAT_ARRAY:
                return DEFAULT_DIM_NULL_VALUE_OF_FLOAT;
            case DOUBLE:
            case DOUBLE_ARRAY:
                return DEFAULT_DIM_NULL_VALUE_OF_DOUBLE;
            case STRING:
            case STRING_ARRAY:
                return DEFAULT_DIM_NULL_VALUE_OF_STRING;
            default:
                throw new UnsupportedOperationException("Unknown default null value for dimension/time column of data type " + dataType);
            }
        default:
            throw new UnsupportedOperationException("Not supported data type for null value - " + dataType);
        }
    }
}
