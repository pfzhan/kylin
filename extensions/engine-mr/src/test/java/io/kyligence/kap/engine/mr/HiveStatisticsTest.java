package io.kyligence.kap.engine.mr;

import org.junit.Test;

import io.kyligence.kap.engine.mr.steps.HiveSampler;
import junit.framework.TestCase;

public class HiveStatisticsTest extends TestCase {
    @Test
    public void testHiveSample() {
        String[] stringValues = { "I love China", "USA", "what is your name", "yes, I like it", "true", "Dinner is perfect", "Not very good" };
        String[] decimalValues = { "1.232323232434", "3.23232323", "-1.3232", "434.223232", "232.22323" };
        HiveSampler sampler = new HiveSampler();
        sampler.setDataType("varchar");

        for (int i = 0; i < stringValues.length; i++) {
            sampler.samples(stringValues[i]);
        }

        sampler.code();
        sampler.getBuffer().flip();
        sampler.decode(sampler.getBuffer());

        assertEquals(sampler.getMax(), "yes, I like it");
        assertEquals(sampler.getMinLenValue().length(), 3);
        sampler.clean();

        sampler = new HiveSampler();
        sampler.setDataType("decimal");

        for (int i = 0; i < decimalValues.length; i++) {
            sampler.samples(decimalValues[i]);
        }

        sampler.code();
        sampler.getBuffer().flip();
        sampler.decode(sampler.getBuffer());

        assertEquals(sampler.getMax(), "434.223232");
        assertEquals(sampler.getMinLenValue().length(), 7);
        sampler.clean();
    }
}
