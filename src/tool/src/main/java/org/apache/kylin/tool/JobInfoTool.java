/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.rest.delegate.JobMetadataInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInfoTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String JOB_INFO_DIR = "job_info";
    private static final String ZIP_SUFFIX = ".zip";

    public static void backup(String dir, String project) throws IOException {
        extractToHDFS(dir + "/" + JOB_INFO_DIR, project);
    }

    public static void restore(String dir, boolean afterTruncate) throws IOException {
        Path path = new Path(dir + "/" + JOB_INFO_DIR);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        for (FileStatus fileStatus : fs.listStatus(path)) {
            String fileName = fileStatus.getPath().getName();
            String project = fileName.substring(0, fileName.indexOf("."));
            restoreProject(dir, project, afterTruncate);
        }
    }

    public static void restoreProject(String dir, String project, boolean afterTruncate) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path path = new Path(dir + "/" + JOB_INFO_DIR + "/" + project + ZIP_SUFFIX);
        List<JobInfo> jobInfos = Lists.newArrayList();
        try(ZipInputStream zis = new ZipInputStream(fs.open(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(zis))) {
            while (zis.getNextEntry() != null) {
                String value = br.readLine();
                JobInfoHelper jobInfo = JsonUtil.readValue(value, JobInfoHelper.class);
                jobInfo.setJobContent();
                jobInfos.add(jobInfo);
            }
        }
        JobMetadataInvoker.getInstance().restoreJobInfo(jobInfos, project, afterTruncate);
    }

    public void extractFull(File dir) {
        JobMapperFilter filter = JobMapperFilter.builder().build();
        List<JobInfo> jobs = JobMetadataInvoker.getInstance().fetchJobList(filter);
        for (JobInfo job : jobs) {
            saveJobToFile(job, dir);
        }
    }

    public void extractFull(File dir, long startTime, long endTime) {
        Date startDate = new Date(startTime);
        Date endDate = new Date(endTime);
        JobMapperFilter filter = JobMapperFilter.builder().timeRange(Arrays.asList(startDate, endDate)).build();
        List<JobInfo> jobs = JobMetadataInvoker.getInstance().fetchJobList(filter);
        for (JobInfo job : jobs) {
            saveJobToFile(job, dir);
        }
    }

    public void extractJob(File dir, String project, String jobId) {
        JobMapperFilter filter = JobMapperFilter.builder().project(project).jobId(jobId).build();
        List<JobInfo> jobs = JobMetadataInvoker.getInstance().fetchJobList(filter);
        if (!jobs.isEmpty()) {
            saveJobToFile(jobs.get(0), dir);
        } else {
            throw new IllegalArgumentException(String.format("Job id {%s} not found.", jobId));
        }
    }

    private void saveJobToFile(JobInfo job, File dir) {
        File jobFile = new File(dir, job.getJobId());
        try (OutputStream os = new FileOutputStream(jobFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            bw.write(JsonUtil.writeValueAsString(new JobInfoHelper(job)));
            bw.newLine();
        } catch (Exception e) {
            logger.error("Write error, id is {}", job.getId(), e);
        }
    }

    public void extractJobLock(File dir) throws Exception {
        List<JobLock> jobLocks = JobMetadataInvoker.getInstance().fetchAllJobLock();
        File jobLockFile = new File(dir, "job_lock");
        try (OutputStream os = new FileOutputStream(jobLockFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            for (JobLock line : jobLocks) {
                try {
                    bw.write(JsonUtil.writeValueAsString(line));
                    bw.newLine();
                } catch (Exception e) {
                    logger.error("Write error, id is {}", line.getId(), e);
                }
            }
        }
    }

    public static void extractToHDFS(String dir, String project) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String filePathStr = dir + "/" + project + ZIP_SUFFIX;
        try (FSDataOutputStream fos = fs.create(new Path(filePathStr));
             ZipOutputStream zos = new ZipOutputStream(fos);
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zos, Charset.defaultCharset()))) {
            JobMapperFilter filter = JobMapperFilter.builder().project(project).build();
            List<JobInfo> jobs = JobMetadataInvoker.getInstance().fetchJobList(filter);
            for (JobInfo job : jobs) {
                zos.putNextEntry(new ZipEntry(job.getJobId() + ".json"));
                String value = JsonUtil.writeValueAsString(new JobInfoHelper(job));
                bw.write(value);
                bw.flush();
            }
        }
    }

    static class JobInfoHelper extends JobInfo {
        private ExecutablePO jobContentJson;

        public JobInfoHelper() {
        }

        public JobInfoHelper(JobInfo jobInfo) {
            this.setId(jobInfo.getId());
            this.setCreateTime(jobInfo.getCreateTime());
            this.setJobDurationMillis(jobInfo.getJobDurationMillis());
            this.setJobId(jobInfo.getJobId());
            this.setJobStatus(jobInfo.getJobStatus());
            this.setJobType(jobInfo.getJobType());
            this.setModelId(jobInfo.getModelId());
            this.setMvcc(jobInfo.getMvcc());
            this.setProject(jobInfo.getProject());
            this.setSubject(jobInfo.getSubject());
            this.setUpdateTime(jobInfo.getUpdateTime());

            jobContentJson = JobInfoUtil.deserializeExecutablePO(jobInfo);
        }

        public ExecutablePO getJobContentJson() {
            return jobContentJson;
        }

        public void setJobContent() {
            this.setJobContent(JobInfoUtil.serializeExecutablePO(jobContentJson));
        }
    }
}