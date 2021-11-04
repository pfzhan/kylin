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

package org.apache.kylin.query.blacklist;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.RandomUtil;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class SQLBlacklistItem implements java.io.Serializable {

    @JsonProperty("id")
    private String id;

    @JsonProperty("regex")
    private String regex;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("concurrent_limit")
    private int concurrentLimit;

    private Pattern pattern;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void updateRandomUuid() {
        setId(RandomUtil.randomUUIDStr());
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
        if (null != this.regex) {
            pattern = Pattern.compile(regex);
        }
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean match(String sql) {
        if (null != this.sql && this.sql.equals(sql)) {
            return true;
        }
        if (null == this.regex) {
            return false;
        }
        if (null == this.pattern) {
            this.pattern = Pattern.compile(regex);
        }
        Matcher matcher = this.pattern.matcher(sql);
        return matcher.matches();
    }

    public int getConcurrentLimit() {
        return concurrentLimit;
    }

    public void setConcurrentLimit(int concurrentLimit) {
        this.concurrentLimit = concurrentLimit;
    }
}
