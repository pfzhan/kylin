package io.kyligence.kap.engine.mr.steps;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class KapInMemCuboidMapper<KEYIN> extends KylinMapper<KEYIN, Object, ByteArrayWritable, ByteArrayWritable> {

    private static final Log logger = LogFactory.getLog(org.apache.kylin.engine.mr.steps.InMemCuboidMapper.class);
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private IMRInput.IMRTableInputFormat flatTableInputFormat;

    private int counter;
    private BlockingQueue<List<String>> queue = new ArrayBlockingQueue<List<String>>(64);
    private Future<?> future;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();

        Map<TblColRef, Dictionary<String>> dictionaryMap = Maps.newHashMap();

        // dictionary
        for (TblColRef col : cubeDesc.getAllColumnsHaveDictionary()) {
            Dictionary<?> dict = cubeSegment.getDictionary(col);
            if (dict == null) {
                logger.warn("Dictionary for " + col + " was not found.");
            }

            dictionaryMap.put(col, cubeSegment.getDictionary(col));
        }

        DoggedCubeBuilder cubeBuilder = new DoggedCubeBuilder(cube.getDescriptor(), dictionaryMap);
        cubeBuilder.setReserveMemoryMB(calculateReserveMB(context.getConfiguration()));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        future = executorService.submit(cubeBuilder.buildAsRunnable(queue, new KapMapContextGTRecordWriter(context, cubeDesc, cubeSegment)));

    }

    private int calculateReserveMB(Configuration configuration) {
        int sysAvailMB = MemoryBudgetController.getSystemAvailMB();
        int mrReserve = configuration.getInt("mapreduce.task.io.sort.mb", 100);
        int sysReserve = Math.max(sysAvailMB / 10, 100);
        int reserveMB = mrReserve + sysReserve;
        logger.info("Reserve " + reserveMB + " MB = " + mrReserve + " (MR reserve) + " + sysReserve + " (SYS reserve)");
        return reserveMB;
    }

    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        String[] row = flatTableInputFormat.parseMapperInput(record);
        List<String> rowAsList = Arrays.asList(row);

        while (!future.isDone()) {
            if (queue.offer(rowAsList, 1, TimeUnit.SECONDS)) {
                counter++;
                if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                    logger.info("Handled " + counter + " records!");
                }
                break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("Totally handled " + counter + " records!");

        while (!future.isDone()) {
            if (queue.offer(Collections.<String> emptyList(), 1, TimeUnit.SECONDS)) {
                break;
            }
        }

        try {
            future.get();
        } catch (Exception e) {
            throw new IOException("Failed to build cube in mapper " + context.getTaskAttemptID().getTaskID().getId(), e);
        }
        queue.clear();
    }

}
