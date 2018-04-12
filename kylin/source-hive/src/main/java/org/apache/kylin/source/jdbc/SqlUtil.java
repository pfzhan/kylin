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

package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Random;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;
import org.apache.kylin.source.hive.DBConnConf;
import org.slf4j.Logger;

public class SqlUtil {
    private static final Logger logger = LoggerFactory.getLogger(SqlUtil.class);

    public static void closeResources(Connection con, Statement statement){
        try{
            if (statement!=null && !statement.isClosed()){
                statement.close();
            }
        }catch(Exception e){
            logger.error("", e);
        }
        
        try{
            if (con!=null && !con.isClosed()){
                con.close();
            }
        }catch(Exception e){
            logger.error("", e);
        }
    }
    
    
    public static void execUpdateSQL(String sql, DataSource ds){
        Connection con = null;
        try{
            con = ds.getConnection();
            execUpdateSQL(con, sql);
        }catch(Exception e){
            logger.error("", e);
        }finally{
            closeResources(con, null);
        }
    }
    
    public static void execUpdateSQL(Connection db, String sql){
        Statement statement=null;
        try{
            statement = db.createStatement();
            statement.executeUpdate(sql);            
        }catch(Exception e){
            logger.error("", e);
        }finally{
            closeResources(null, statement);
        }
    }
    
    public static int tryTimes=10;
    public static Connection getConnection(DBConnConf dbconf){
        if (dbconf.getUrl()==null)
            return null;
        Connection con = null;
        try {
            Class.forName(dbconf.getDriver());
        }catch(Exception e){
            logger.error("", e);
        }
        boolean got=false;
        int times=0;
        Random r = new Random();
        while(!got && times<tryTimes){
            times++;
            try {
                con = DriverManager.getConnection(dbconf.getUrl(), dbconf.getUser(), dbconf.getPass());
                got = true;
            }catch(Exception e){
                logger.warn("while use:" + dbconf, e);
                try {
                    int rt = r.nextInt(10);
                    Thread.sleep(rt*1000);
                } catch (InterruptedException e1) {
                }
            }
        }
        return con;
    }
}
