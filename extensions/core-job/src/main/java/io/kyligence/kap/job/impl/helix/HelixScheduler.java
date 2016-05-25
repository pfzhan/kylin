package io.kyligence.kap.job.impl.helix;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.lock.JobLock;

public class HelixScheduler implements Scheduler<AbstractExecutable> {

    HelixClusterAdmin clusterAdmin = null;

    boolean started = false;

    private static HelixScheduler INSTANCE = null;

    public HelixScheduler() {
        if (INSTANCE != null) {
            throw new IllegalStateException("DefaultScheduler has been initiated.");
        }
        INSTANCE = this;
    }

    @Override
    public void init(JobEngineConfig jobEngineConfig, JobLock jobLock) throws SchedulerException {
        final KylinConfig kylinConfig = jobEngineConfig.getConfig();
        clusterAdmin = HelixClusterAdmin.getInstance(kylinConfig);
        clusterAdmin.start();
        started = true;
    }

    @Override
    public void shutdown() throws SchedulerException {
        if (clusterAdmin != null) {
            clusterAdmin.stop();
        }
        started = false;
    }

    @Override
    public boolean stop(AbstractExecutable executable) throws SchedulerException {
        return false;
    }

    @Override
    public boolean hasStarted() {
        return started;
    }
}
