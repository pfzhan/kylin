package io.kyligence.kap.tool.hive.udf;

import org.apache.hadoop.io.Text;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

/**
 * Created by lingyanjiang on 16/6/24.
 */
public class ObfuscateUDF extends AbstractUdf {
    private static final String NAME = "kylin_obfuscate";

    public Text evaluate(Text value) {
        if (value == null)
            return null;
        return new Text(String.valueOf(Hashing.murmur3_32().newHasher().putString(value.toString(), Charsets.UTF_8).hash().asInt()));
    }

    @Override
    public String getFuncName() {
        return NAME;
    }
}
