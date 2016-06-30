package io.kyligence.kap.tool.udf;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by lingyanjiang on 16/6/24.
 */
public class ObfuscateUdf extends UDF {
    public Text evaluate(Text value) {
        if(value == null) return null;
        return new Text(String.valueOf(Hashing.murmur3_32().newHasher().putString(value.toString(), Charsets.UTF_8).hash().asInt()));
    }
}
