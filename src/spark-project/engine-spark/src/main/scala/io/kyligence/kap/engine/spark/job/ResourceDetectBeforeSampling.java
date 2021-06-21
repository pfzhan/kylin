package io.kyligence.kap.engine.spark.job;

import com.google.common.collect.Maps;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import scala.collection.JavaConversions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import scala.collection.JavaConverters;

@Slf4j
public class ResourceDetectBeforeSampling extends SparkApplication {
    @Override
    protected void doExecute() {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        final TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        LinkedHashMap<String, String> params = NProjectManager.getInstance(config).getProject(project).getOverrideKylinProps();
        long rowCount = Long.parseLong(getParam(NBatchConstants.P_SAMPLING_ROWS));
        params.put("sampleRowCount", String.valueOf(rowCount));
        final Dataset<Row> dataset = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, params);
        final List<Path> paths = JavaConversions
                .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan()));

        Map<String, Long> resourceSize = Maps.newHashMap();
        resourceSize.put(String.valueOf(tableName),
            ResourceDetectUtils.getResourceSize(
                JavaConverters.asScalaIteratorConverter(paths.iterator()).asScala().toSeq()));

        Map<String, String> tableLeafTaskNums = Maps.newHashMap();
        tableLeafTaskNums.put(tableName,
                ResourceDetectUtils.getPartitions(dataset.queryExecution().executedPlan()));

        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), tableName + "_" + ResourceDetectUtils.fileName()),
            resourceSize);

        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix()), tableLeafTaskNums);
    }

    public static void main(String[] args) {
        ResourceDetectBeforeSampling detect = new ResourceDetectBeforeSampling();
        detect.execute(args);
    }
}
