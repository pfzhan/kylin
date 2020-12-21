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
package io.kyligence.kap.tool.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.spark.sql.SparderEnv;

import com.google.common.base.Preconditions;

import io.kyligence.kap.query.util.ExtractFactory;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ToolUtil {

    private ToolUtil() {
    }

    public static void dumpKylinJStack(File outputFile) throws IOException, ShellException {
        String jstackDumpCmd = String.format(Locale.ROOT, "jstack -l %s", getKylinPid());
        val result = new CliCommandExecutor().execute(jstackDumpCmd, null);
        FileUtils.writeStringToFile(outputFile, result.getCmd());
    }

    public static String getKylinPid() {
        File pidFile = new File(getKylinHome(), "pid");
        if (pidFile.exists()) {
            try {
                return FileUtils.readFileToString(pidFile);
            } catch (IOException e) {
                throw new RuntimeException("Error reading KYLIN PID file.", e);
            }
        }
        throw new RuntimeException("Cannot find KYLIN PID file.");
    }

    public static String getKylinHome() {
        String path = System.getProperty(KylinConfig.KYLIN_CONF);
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        path = KylinConfig.getKylinHome();
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        throw new RuntimeException("Cannot find KYLIN_HOME.");
    }

    public static String getBinFolder() {
        final String BIN = "bin";
        return getKylinHome() + File.separator + BIN;
    }

    public static String getLogFolder() {
        final String LOG = "logs";
        return getKylinHome() + File.separator + LOG;
    }

    public static String getConfFolder() {
        final String CONF = "conf";
        return getKylinHome() + File.separator + CONF;
    }

    public static String getHadoopConfFolder() {
        final String HADOOP_CONF = "hadoop_conf";
        return getKylinHome() + File.separator + HADOOP_CONF;
    }

    public static String getMetaStoreId() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ResourceStore store = ResourceStore.getKylinMetaStore(kylinConfig);
        return store.getMetaStoreUUID();
    }

    public static String getHostName() {
        String hostname = System.getenv("COMPUTERNAME");
        if (StringUtils.isEmpty(hostname)) {
            try {
                InetAddress address = InetAddress.getLocalHost();
                hostname = address.getHostName();
                if (StringUtils.isEmpty(hostname)) {
                    hostname = address.getHostAddress();
                }
            } catch (UnknownHostException uhe) {
                String host = uhe.getMessage(); // host = "hostname: hostname"
                if (host != null) {
                    int colon = host.indexOf(':');
                    if (colon > 0) {
                        return host.substring(0, colon);
                    }
                }
                hostname = "Unknown";
            }
        }
        return hostname;
    }

    private static String getHdfsPrefix() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return kylinConfig.getHdfsWorkingDirectory();
    }

    public static String getSparderLogsDir() {
        final String SPARDER_LOG = "_sparder_logs";
        return getHdfsPrefix() + File.separator + SPARDER_LOG;
    }

    public static String getSparkLogsDir(String project) {
        Preconditions.checkArgument(!StringUtils.isBlank(project));

        final String SPARK_LOG = "spark_logs";
        return getHdfsPrefix() + File.separator + project + File.separator + SPARK_LOG;
    }

    public static String getJobTmpDir(String project, String jobId) {
        Preconditions.checkArgument(!StringUtils.isBlank(project) && !StringUtils.isBlank(jobId));

        final String JOB_TMP = "job_tmp";
        return getHdfsPrefix() + File.separator + project + File.separator + JOB_TMP + File.separator + jobId;
    }

    public static boolean waitForSparderRollUp() {
        boolean isRollUp = false;
        if (!KapConfig.wrap(KylinConfig.getInstanceFromEnv()).isCloud()) {
            log.info("Failed to roll up eventLog because environment is not in cloud.");
            return isRollUp;
        }
        val extractor = ExtractFactory.create();
        String check = SparderEnv.rollUpEventLog();
        if (StringUtils.isBlank(check)) {
            log.info("Failed to roll up eventLog because the spader is closed.");
            return isRollUp;
        }
        String logDir = extractor.getSparderEvenLogDir();
        ExecutorService es = Executors.newSingleThreadExecutor();
        FileSystem fs = HadoopUtil.getFileSystem(logDir);
        try {
            Future<Boolean> task = es.submit(() -> {
                while (true) {
                    if (fs.exists(new Path(logDir, check))) {
                        return true;
                    }
                    Thread.sleep(1000);
                }
            });
            if (task.get(10, TimeUnit.SECONDS)) {
                fs.delete(new Path(logDir, check), false);
                isRollUp = true;
            }
        } catch (Exception e) {
            log.warn("Sparder eventLog rollUp failed.", e);
        } finally {
            es.shutdown();
        }
        return isRollUp;
    }

    public static boolean isPortAvailable(String ip, int port) {
        boolean isAvailable;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), 1000);
            isAvailable = socket.isConnected();
        } catch (Exception e) {
            log.warn("Connect failed", e);
            isAvailable = false;
        }
        return isAvailable;
    }
}
