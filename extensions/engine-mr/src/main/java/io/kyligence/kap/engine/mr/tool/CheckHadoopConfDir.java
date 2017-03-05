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

package io.kyligence.kap.engine.mr.tool;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class CheckHadoopConfDir {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(false); // don't load defaults, we are only interested in the specified conf dir
        File hadoopConfDir = new File(args[0]).getCanonicalFile();

        System.out.println("Checking hadoop conf dir " + hadoopConfDir);
        
        if (hadoopConfDir.exists() == false) {
            System.err.println("ERROR: Hadoop conf dir '" + hadoopConfDir + "' does not exist");
            System.exit(1);
        }

        if (hadoopConfDir.isDirectory() == false) {
            System.err.println("ERROR: Hadoop conf dir '" + hadoopConfDir + "' is not a directory");
            System.exit(1);
        }
        
        LocalFileSystem localfs = FileSystem.getLocal(conf);
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
            System.out.println("WARN: 'dfs.client.read.shortcircuit' is not enabled. Check " + hadoopConfDir + "/hdfs-site.xml");
        }
        
        System.exit(0);
    }
}
