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

package io.kyligence.kap.tool.storage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.kylin.common.util.HadoopUtil;

import com.google.common.collect.Lists;

public class KapTestDistcpCLI {
    private static Path localPath = new Path("hdfs:///tmp/distcp_test");
    private static Path remotePath;

    public static void main(String[] args) throws Exception {
        writeLocalHdfsFile(args);

        if (args.length != 2) {
            System.out.println(
                    "Usage:\n" + "\tbin/kylin.sh " + KapTestDistcpCLI.class.getName() + " -keep <remote hdfs path>\n"
                            + "\tbin/kylin.sh " + KapTestDistcpCLI.class.getName() + " -nokeep <remote hdfs path>\n");
            return;
        }

        remotePath = new Path(args[1]);
        if (args[0].equals("-keep")) {
            scp(true, localPath, remotePath);
        } else if (args[0].equals("-nokeep")) {
            scp(false, localPath, remotePath);
        }
    }

    private static void writeLocalHdfsFile(String[] args) throws IOException {
        Configuration config = HadoopUtil.getCurrentConfiguration();
        int blockSize = 1024 * 1024; // 1M
        config.set("dfs.blocksize", String.valueOf(blockSize));
        FileSystem fs = FileSystem.get(config);

        if (fs.exists(localPath)) {
            fs.delete(localPath, true);
        }

        FSDataOutputStream fout = fs.create(localPath);
        byte[] buffer = new byte[blockSize];
        for (int i = 0; i < 10; i++) {
            fout.write(buffer);
        }
        fout.close();
    }

    private static void scp(boolean keep, Path localPath, Path remotePath) throws Exception {
        DistCpOptions options = new DistCpOptions(Lists.newArrayList(localPath), remotePath);
        if (keep) {
            options.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
        }
        options.setBlocking(true);
        DistCp distCp = new DistCp(HadoopUtil.getCurrentConfiguration(), options);
        distCp.execute();
    }
}
