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
