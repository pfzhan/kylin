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

package io.kyligence.kap.cube.raw;

import org.apache.kylin.common.util.ImmutableBitSet;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.datatype.OrderedBytesStringSerializer;

public class RawValueIngester {

    //TODO: null values are silently transformed to default values
    public static Object[] buildObjectOf(String[] values, BufferedRawColumnCodec columnCodec, ImmutableBitSet activeCols) {
        Preconditions.checkArgument(values.length == columnCodec.getColumnsCount());

        Object[] objects = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            if (activeCols.get(i)) {
                //special treatment for hive null values, only string will reserve null values
                //other types will be changed to 0
                if (!"\\N".equals(values[i])) {
                    objects[i] = columnCodec.getDataTypeSerializer(i).valueOf(values[i]);
                } else {
                    if (columnCodec.getDataTypeSerializer(i) instanceof OrderedBytesStringSerializer) {
                        objects[i] = null;
                    } else {
                        objects[i] = columnCodec.getDataTypeSerializer(i).valueOf("0");
                    }
                }
            }
        }
        return objects;
    }
}
