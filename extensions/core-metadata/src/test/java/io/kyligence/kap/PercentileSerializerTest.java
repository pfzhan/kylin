package io.kyligence.kap;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.measure.percentile.PercentileCounter;
import io.kyligence.kap.measure.percentile.PercentileSerializer;

/**
 * Created by dongli on 5/21/16.
 */
public class PercentileSerializerTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testBasic() {
        PercentileSerializer serializer = new PercentileSerializer(DataType.getType("percentile(100)"));
        PercentileCounter counter = new PercentileCounter(100, 0.5);
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            counter.add(random.nextDouble());
        }
        double markResult = counter.getResultEstimate();

        ByteBuffer buffer = ByteBuffer.allocateDirect(serializer.getStorageBytesEstimate());
        serializer.serialize(counter, buffer);

        buffer.flip();
        counter = serializer.deserialize(buffer);
        PercentileCounter counter1 = new PercentileCounter(100, 0.5);
        counter1.merge(counter);

        assertEquals(markResult, counter1.getResultEstimate(), 0.001);
    }

}
