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

package io.kyligence.kap.storage.parquet.format.serialize;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class RoaringBitmaps {
    public static String writeToString(Iterable<Integer> bits) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i : bits) {
            bitmap.add(i);
        }
        ByteArrayOutputStream bos = null;
        DataOutputStream dos = null;
        String result = null;
        try {
            bos = new ByteArrayOutputStream();
            dos = new DataOutputStream(bos);
            bitmap.serialize(dos);
            dos.flush();
            result = new String(bos.toByteArray(), "ISO-8859-1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
        }
        return result;
    }

    public static ImmutableRoaringBitmap readFromString(String pageBitsetString) throws IOException {
        ImmutableRoaringBitmap bitmap = null;

        if (pageBitsetString != null) {
            ByteBuffer buf = ByteBuffer.wrap(pageBitsetString.getBytes("ISO-8859-1"));
            bitmap = new ImmutableRoaringBitmap(buf);
        }
        return bitmap;
    }
}
