/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.job.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.dao.ExecutablePO;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import io.kyligence.kap.job.domain.JobInfo;
import lombok.extern.slf4j.Slf4j;

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
