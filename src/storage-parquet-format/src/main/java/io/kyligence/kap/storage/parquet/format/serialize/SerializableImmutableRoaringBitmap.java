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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerializableImmutableRoaringBitmap implements Serializable, KryoSerializable {
    private static final long serialVersionUID = 4392777396404406037L;

    private transient ImmutableRoaringBitmap bitmap;

    @Override
    public String toString() {
        return "bitmap=" + bitmap.toString();
    }

    public SerializableImmutableRoaringBitmap(ImmutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    public ImmutableRoaringBitmap getBitmap() {
        return bitmap;
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();

        int bufferLength = ois.readInt();
        byte[] buffer = new byte[bufferLength];
        ois.read(buffer);

        bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(buffer));
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();

        byte[] buffer = null;
        ByteArrayOutputStream baos = null;
        DataOutputStream dos = null;
        try {
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
            bitmap.serialize(dos);
            buffer = baos.toByteArray();
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(baos);
        }

        oos.writeInt(buffer.length);
        oos.write(buffer);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            bitmap.serialize(dos);
            kryo.writeObject(output, baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(baos);
            IOUtils.closeQuietly(dos);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        byte[] buffer = kryo.readObject(input, byte[].class);
        bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(buffer));
    }
}
