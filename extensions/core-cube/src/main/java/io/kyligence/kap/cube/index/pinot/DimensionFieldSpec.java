/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.cube.index.pinot;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 */
public class DimensionFieldSpec extends FieldSpec {

    public DimensionFieldSpec() {
        super();
        setFieldType(FieldType.DIMENSION);
    }

    public DimensionFieldSpec(String name, DataType dType, boolean singleValue, String delimeter) {
        super(name, FieldType.DIMENSION, dType, singleValue, delimeter);
    }

    public DimensionFieldSpec(String name, DataType dType, boolean singleValue) {
        super(name, FieldType.DIMENSION, dType, singleValue);
    }

}
