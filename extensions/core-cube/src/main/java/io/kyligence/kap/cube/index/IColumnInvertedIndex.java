/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.cube.index;

import java.io.Closeable;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public interface IColumnInvertedIndex<T> {

    Builder rebuild();

    Reader getReader();

    public interface Builder<T> extends Closeable {

        // one integer per row
        void putNextRow(T value);

        // several integers per row
        void putNextRow(T[] value);

        void appendToRow(T value, int row);

        void appendToRow(T[] value, int row);
    }

    public interface Reader<T> extends Closeable {

        ImmutableRoaringBitmap getRows(T v);

        int getNumberOfRows();
    }
}
