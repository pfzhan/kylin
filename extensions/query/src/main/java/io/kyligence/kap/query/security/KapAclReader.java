/**
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

package io.kyligence.kap.query.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapAclReader extends TimerTask implements IACLMetaData {
    private static final Logger logger = LoggerFactory.getLogger(KapAclReader.class);
    private static final Timer timer = new Timer(true);
    private String filename;
    private long preLastModified = 0L;

    public KapAclReader() {
        String path;
        if (null != System.getenv("KYLIN_HOME"))
            path = System.getenv("KYLIN_HOME") + File.separator + "conf";
        else
            path = System.getProperty(KylinConfig.KYLIN_CONF);

        filename = path + File.separator + KapConfig.getInstanceFromEnv().getCellLevelSecurityConfig();

        try {
            loadACLFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // interval set as 3 seconds
        timer.scheduleAtFixedRate(this, 5000, 3 * 1000);
    }

    @Override
    public synchronized boolean loadACLFile() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(this.filename));
        String line = reader.readLine();
        if (null == line) {
            reader.close();
            return false;
        }
        clearAll();
        String[] cols = line.split(IACLMetaData.CSV_SPLIT);
        //collect columns name
        for (int i = 0; i < cols.length; i++)
            IACLMetaData.allAclColumns.add(cols[i]);
        //scan acl table
        while (null != (line = reader.readLine())) {
            Hashtable<String, String> limitedColumns = new Hashtable<>();
            String[] values = line.split(IACLMetaData.CSV_SPLIT);
            for (int i = 0; i < values.length; i++) {
                limitedColumns.put(cols[i].toLowerCase(), values[i].trim());
            }
            IACLMetaData.accessControlColumnsByUser.put(values[0].toLowerCase(), limitedColumns);
        }
        reader.close();
        return true;
    }

    @Override
    public void run() {
        if (isACLChanged())
            try {
                loadACLFile();
                logger.info("Reload acl file");
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public boolean isACLChanged() {
        long last = lastModifiedACL();
        if (this.preLastModified != last) {
            this.preLastModified = last;
            return true;
        }
        return false;
    }

    public long lastModifiedACL() {
        File file = new File(this.filename);
        long last = file.lastModified();
        return last;
    }

    public void clearAll() {
        for (Map.Entry<String, Hashtable<String, String>> columnPair : IACLMetaData.accessControlColumnsByUser.entrySet()) {
            columnPair.getValue().clear();
        }
        IACLMetaData.accessControlColumnsByUser.clear();
        IACLMetaData.allAclColumns.clear();
    }
}
