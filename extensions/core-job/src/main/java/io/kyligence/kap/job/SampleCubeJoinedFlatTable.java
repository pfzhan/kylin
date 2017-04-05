package io.kyligence.kap.job;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

/**
 * Created by luwei on 17-3-24.
 */
public class SampleCubeJoinedFlatTable extends JoinedFlatTable {

    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc, JobEngineConfig engineConfig) {
        StringBuilder sql = new StringBuilder();

        sql.append(generateHiveSetStatements(engineConfig));
        sql.append("INSERT OVERWRITE TABLE " + flatDesc.getTableName() + " " + generateSelectDataStatement(flatDesc));
        appendAdvancedHiveStatement(flatDesc, sql);
        sql.append(";\n");

        return sql.toString();
    }

    private static void appendAdvancedHiveStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql) {
        final KylinConfig kylinConfig = ((CubeSegment) flatDesc.getSegment()).getConfig();
        final KapConfig kapConfig = KapConfig.wrap(kylinConfig);

        if (kapConfig.isAdvancedFlatTableByRowNum()) {
            int rowNum = kapConfig.getAdvancedFlatTableRowNum();
            sql.append("DISTRIBUTE BY RAND() SORT BY RAND() LIMIT " + rowNum).append(";\n");
        } else {
            int percentage = kapConfig.getAdvancedFlatTablePercentage();
            double percent = (double) percentage / 100;
            if (sql.toString().contains("WHERE")) {
                sql.delete(sql.lastIndexOf(")"), sql.length());
                sql.append(" AND ");
            } else
                sql.append("WHERE (");
            sql.append("RAND() < " + percent).append(");\n");
        }
    }
}