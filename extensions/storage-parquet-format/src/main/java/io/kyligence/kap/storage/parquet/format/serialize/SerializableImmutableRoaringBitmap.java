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

/**
 * Created by roger on 6/21/16.
 */
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
