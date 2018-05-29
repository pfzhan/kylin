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

package io.kyligence.kap.storage.parquet.format.file;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

public abstract class GeneralValuesReader {
    private ValuesReader valuesReader;
    private int length;
    private int curPos;

    public GeneralValuesReader(ValuesReader reader, int length) {
        valuesReader = reader;
        this.length = length;
        curPos = 0;
    }

    public void skip() {
        if (curPos == length) {
            return;
        }

        curPos++;
        valuesReader.skip();
    }

    public int getLength() {
        return length;
    }

    public Integer getNextOffset() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.getNextOffset();
    }

    public Integer readValueDictionaryId() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readValueDictionaryId();
    }

    public Boolean readBoolean() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readBoolean();
    }

    public Binary readBytes() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readBytes();
    }

    public Float readFloat() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readFloat();
    }

    public Double readDouble() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readDouble();
    }

    public Integer readInteger() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readInteger();
    }

    public Long readLong() {
        if (curPos == length) {
            return null;
        }
        curPos++;
        return valuesReader.readLong();
    }

    public abstract Object readData();

    public static class Builder {
        private PrimitiveType.PrimitiveTypeName type = BINARY;
        private ValuesReader reader = null;
        private int length = -1;

        public Builder setLength(int length) {
            this.length = length;
            return this;
        }

        public Builder setType(PrimitiveType.PrimitiveTypeName type) {
            this.type = type;
            return this;
        }

        public Builder setReader(ValuesReader reader) {
            this.reader = reader;
            return this;
        }

        public GeneralValuesReader build() {
            if (length < 0) {
                throw new IllegalStateException("Values Reader's length should be");
            }

            if (reader == null) {
                throw new IllegalStateException("Values Reader should not be null");
            }

            switch (type) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return new GeneralValuesReader(reader, length) {
                    @Override
                    public Object readData() {
                        return this.readBytes();
                    }
                };
            case INT32:
                return new GeneralValuesReader(reader, length) {
                    @Override
                    public Object readData() {
                        return this.readInteger();
                    }
                };
            case INT64:
                return new GeneralValuesReader(reader, length) {
                    @Override
                    public Object readData() {
                        return this.readLong();
                    }
                };
            case BOOLEAN:
                return new GeneralValuesReader(reader, length) {
                    @Override
                    public Object readData() {
                        return this.readBoolean();
                    }
                };
            case DOUBLE:
                return new GeneralValuesReader(reader, length) {
                    @Override
                    public Object readData() {
                        return this.readDouble();
                    }
                };
            case FLOAT:
                return new GeneralValuesReader(reader, length) {
                    @Override
                    public Object readData() {
                        return this.readFloat();
                    }
                };
            default:
                return null;
            }
        }
    }
}
