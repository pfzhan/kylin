package io.kyligence.kap.job.util;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.job.domain.JobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.dao.ExecutablePO;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class JobInfoUtil {

    public static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<>(ExecutablePO.class);

    public static byte[] serializeExecutablePO(ExecutablePO executablePO) {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream dout = new DataOutputStream(buf);) {
            JOB_SERIALIZER.serialize(executablePO, dout);
            ByteSource byteSource = ByteSource.wrap(buf.toByteArray());
            return byteSource.read();
        } catch (IOException e) {
            throw new RuntimeException("Serialize ExecutablePO failed, id: " + executablePO.getId(), e);
        }
    }

    public static ExecutablePO deserializeExecutablePO(JobInfo jobInfo) {
        ByteSource byteSource = ByteSource.wrap(jobInfo.getJobContent());
        try (InputStream is = byteSource.openStream(); DataInputStream din = new DataInputStream(is)) {
            ExecutablePO r = JOB_SERIALIZER.deserialize(din);
            r.setLastModified(jobInfo.getUpdateTime().getTime());
            r.setProject(jobInfo.getProject());
            return r;
        } catch (IOException e) {
            log.warn("Error when deserializing jobInfo, id: {} " + jobInfo.getJobId(), e);
            return null;
        }
    }


}
