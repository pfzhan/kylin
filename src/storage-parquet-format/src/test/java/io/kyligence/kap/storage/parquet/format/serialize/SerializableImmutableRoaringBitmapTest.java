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

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class SerializableImmutableRoaringBitmapTest {
    @Test
    public void testDeSer() throws IOException, ClassNotFoundException {
        SerializableImmutableRoaringBitmap bitmap = new SerializableImmutableRoaringBitmap(ImmutableRoaringBitmap.bitmapOf());

        File tmpFile = File.createTempFile("tmp", ".bitmap");
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tmpFile));
        oos.writeObject(bitmap);
        oos.close();

        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tmpFile));
        SerializableImmutableRoaringBitmap result = (SerializableImmutableRoaringBitmap) ois.readObject();
        ois.close();

        assertArrayEquals(bitmap.getBitmap().toArray(), result.getBitmap().toArray());

        tmpFile.delete();
    }
}
