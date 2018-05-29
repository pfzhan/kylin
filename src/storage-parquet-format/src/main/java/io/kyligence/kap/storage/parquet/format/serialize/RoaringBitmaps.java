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

    public static byte[] writeToBytes(MutableRoaringBitmap bitmap) {
        ByteArrayOutputStream bos = null;
        DataOutputStream dos = null;
        byte[] result = null;
        try {
            bos = new ByteArrayOutputStream();
            dos = new DataOutputStream(bos);
            bitmap.serialize(dos);
            dos.flush();
            result = bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
        }
        return result;
    }

    public static MutableRoaringBitmap readFromBytes(byte[] bytes) {
        MutableRoaringBitmap bitmap = null;

        if (bytes != null) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            bitmap = new ImmutableRoaringBitmap(buf).toMutableRoaringBitmap();
        }
        return bitmap;
    }
}
