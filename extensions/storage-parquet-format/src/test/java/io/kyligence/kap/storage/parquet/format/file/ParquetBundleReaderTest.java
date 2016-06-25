package io.kyligence.kap.storage.parquet.format.file;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class ParquetBundleReaderTest extends AbstractParquetFormatTest {
    public ParquetBundleReaderTest() throws IOException {
        super();
    }

    @Test
    public void ReadInBundle() throws Exception {
        writeRows(groupSize - 1);

        ImmutableRoaringBitmap bitset = createBitset(10);
        bitset = deserialize(serialize(bitset));

        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(path).setConf(new Configuration()).setPageBitset(bitset).build();
        List<Object> data = bundleReader.read();
        Assert.assertNotNull(data);
        Assert.assertArrayEquals(((Binary) data.get(0)).getBytes(), new byte[] { 2, 3 });

        for (int i = 0; i < (10 * ParquetConfig.RowsPerPage - 1); ++i) {
            bundleReader.read();
        }

        Assert.assertNull(bundleReader.read());
        bundleReader.close();
    }

    private static ImmutableRoaringBitmap createBitset(int total) throws IOException {
        MutableRoaringBitmap mBitmap = new MutableRoaringBitmap();
        for (int i = 0; i < total; ++i) {
            mBitmap.add(i);
        }

        ImmutableRoaringBitmap iBitmap = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos);) {
            mBitmap.serialize(dos);
            dos.flush();
            iBitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(baos.toByteArray()));
        }

        return iBitmap;
    }

    private String serialize(ImmutableRoaringBitmap bitset) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitset.serialize(dos);
        dos.close();

        return new String(bos.toByteArray());
    }

    private ImmutableRoaringBitmap deserialize(String str) {
        return new ImmutableRoaringBitmap(ByteBuffer.wrap(str.getBytes()));
    }
}
