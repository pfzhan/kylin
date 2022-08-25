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

package org.apache.kylin.job.domain;

import java.util.Date;

public class JobLock {
    private Long id;

    private String lockId;

    private String lockNode;

    private Date lockExpireTime;

    private Date createTime;

    private Date updateTime;

    // placeholder for mybatis ${}
    private String jobLockTable;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getLockId() {
        return lockId;
    }

    public void setLockId(String lockId) {
        this.lockId = lockId;
    }

    public String getLockNode() {
        return lockNode;
    }

    public void setLockNode(String lockNode) {
        this.lockNode = lockNode;
    }

    public Date getLockExpireTime() {
        return lockExpireTime;
    }

    public void setLockExpireTime(Date lockExpireTime) {
        this.lockExpireTime = lockExpireTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getJobLockTable() {
        return jobLockTable;
    }

    public void setJobLockTable(String jobLockTable) {
        this.jobLockTable = jobLockTable;
    }
}