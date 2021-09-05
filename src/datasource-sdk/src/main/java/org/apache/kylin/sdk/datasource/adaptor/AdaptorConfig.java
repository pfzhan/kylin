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
package org.apache.kylin.sdk.datasource.adaptor;

public class AdaptorConfig {
    public final String url;
    public final String driver;
    public final String username;
    public final String password;

    public String datasourceId;
    public int poolMaxIdle = 10;
    public int poolMaxTotal = 10;
    public int poolMinIdle = 2;
    public long timeBetweenEvictionRunsMillis = 15000;
    public long maxWait = 60000;

    private int connectRetryTimes = 1;
    private long sleepMillisecBetweenRetry = 100;

    public AdaptorConfig(String url, String driver, String username, String password) {
        this.url = url;
        this.driver = driver;
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AdaptorConfig that = (AdaptorConfig) o;

        if (poolMaxIdle != that.poolMaxIdle)
            return false;
        if (poolMaxTotal != that.poolMaxTotal)
            return false;
        if (poolMinIdle != that.poolMinIdle)
            return false;
        if (!url.equals(that.url))
            return false;
        if (!driver.equals(that.driver))
            return false;
        if (!username.equals(that.username))
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        return datasourceId != null ? datasourceId.equals(that.datasourceId) : that.datasourceId == null;
    }

    @Override
    public int hashCode() {
        int result = url.hashCode();
        result = 31 * result + driver.hashCode();
        result = 31 * result + username.hashCode();
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (datasourceId != null ? datasourceId.hashCode() : 0);
        result = 31 * result + poolMaxIdle;
        result = 31 * result + poolMaxTotal;
        result = 31 * result + poolMinIdle;
        return result;
    }

    public int getConnectRetryTimes() {
        return connectRetryTimes;
    }

    public void setConnectRetryTimes(int connectRetryTimes) {
        this.connectRetryTimes = connectRetryTimes;
    }

    public long getSleepMillisecBetweenRetry() {
        return sleepMillisecBetweenRetry;
    }

    public void setSleepMillisecBetweenRetry(long sleepMillisecBetweenRetry) {
        this.sleepMillisecBetweenRetry = sleepMillisecBetweenRetry;
    }
}
