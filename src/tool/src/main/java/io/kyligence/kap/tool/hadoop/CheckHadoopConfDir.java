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

package io.kyligence.kap.tool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class CheckHadoopConfDir {
    public static String CHECKENV_REPORT_PFX = ">   ";

    public static void main(String[] args) throws Exception {

        if (1 != args.length) {
            usage();
            System.exit(1);
        }

        File hadoopConfDir = new File(args[0]).getCanonicalFile();

        System.out.println("Checking hadoop config dir " + hadoopConfDir);

        if (hadoopConfDir.exists() == false) {
            System.err.println("ERROR: Hadoop config dir '" + hadoopConfDir + "' does not exist");
            System.exit(1);
        }

        if (hadoopConfDir.isDirectory() == false) {
            System.err.println("ERROR: Hadoop config dir '" + hadoopConfDir + "' is not a directory");
            System.exit(1);
        }

        LocalFileSystem localfs = getLocalFSAndHitUGIForTheFirstTime();

        Configuration conf = new Configuration(false); // don't load defaults, we are only interested in the specified config dir
        for (File f : hadoopConfDir.listFiles()) {
            if (f.getName().endsWith("-site.xml")) {
                Path p = new Path(f.toString());
                p = localfs.makeQualified(p);
                conf.addResource(p);
                System.out.println("Load " + p);
            }
        }
        conf.reloadConfiguration();

        boolean shortcircuit = conf.getBoolean("dfs.client.read.shortcircuit", false);
        if (shortcircuit == false) {
            System.out.println(CHECKENV_REPORT_PFX + "WARN: 'dfs.client.read.shortcircuit' is not enabled which could impact query performance. Check " + hadoopConfDir + "/hdfs-site.xml");
        }

        System.exit(0);
    }

    /*
     * Although this is getting a LocalFileSystem, but it triggers Hadoop security check inside.
     * This is the very first time we hit UGI during the check-env process, and could hit Kerberos exception in a secured Hadoop.
     * Be careful about the error reporting.
     */
    private static LocalFileSystem getLocalFSAndHitUGIForTheFirstTime() throws IOException {
        try {
            LocalFileSystem localfs = FileSystem.getLocal(new Configuration());
            return localfs;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("ERROR: Hadoop security exception? Seems the classpath is not setup propertly regarding Hadoop security.");
            System.exit(1);
            return null;
        }
    }

    private static void usage() {
        System.out.println("Usage: CheckHadoopConfDir hadoopConfDir");
    }
}
