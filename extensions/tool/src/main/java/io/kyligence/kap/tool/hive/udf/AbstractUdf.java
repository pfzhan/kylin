package io.kyligence.kap.tool.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public abstract class AbstractUdf extends UDF {
    public abstract String getFuncName();
}
