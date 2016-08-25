package io.kyligence.kap.storage.parquet.steps;

import java.util.ArrayList;
import java.util.List;

public class RawTableUtils {
    public static List<byte[]> hash(List<byte[]> value) {
        List<byte[]> result = new ArrayList<>(value.size());
        for (byte[] v: value) {
            result.add(hash(v));
        }
        return result;
    }

    public static byte[] hash(byte[] value) {
        byte[] result = new byte[8];

        if (value.length <= 8) {
            System.arraycopy(value, 0, result, 0, value.length);
            for (int i = value.length; i < 8; i++) {
                result[i] = (byte)0;
            }
        } else {
            System.arraycopy(value, 0, result, 0, 8);

            for (int i = 8; i < value.length; i++) {
                result[i % 8] ^= value[i];
            }
        }

        return result;
    }
}
