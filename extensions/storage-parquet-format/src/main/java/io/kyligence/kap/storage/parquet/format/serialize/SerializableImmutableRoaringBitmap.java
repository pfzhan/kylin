package io.kyligence.kap.storage.parquet.format.serialize;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * Created by roger on 6/21/16.
 */
public class SerializableImmutableRoaringBitmap implements Serializable {
    private static final long serialVersionUID = 4392777396404406037L;

    private transient ImmutableRoaringBitmap bitmap;
    private transient byte[] buffer;

    public SerializableImmutableRoaringBitmap(ImmutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    public ImmutableRoaringBitmap getBitmap() {
        return bitmap;
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();

        int bufferLength = ois.readInt();
        buffer = new byte[bufferLength];
        ois.read(buffer);

        bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(buffer));
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        bitmap.serialize(dos);
        buffer = baos.toByteArray();

        oos.write(buffer.length);
        oos.write(buffer);
    }
}
