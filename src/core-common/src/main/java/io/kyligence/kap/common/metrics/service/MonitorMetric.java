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
package io.kyligence.kap.common.metrics.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.kyligence.kap.shaded.influxdb.org.influxdb.annotation.Column;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class MonitorMetric implements MonitorMetricOperation {
    @JsonProperty("host")
    @Column(name = "host", tag = true)
    private String host;

    @JsonProperty
    @Column(name = "ip", tag = true)
    private String ip;

    @JsonProperty("port")
    @Column(name = "port", tag = true)
    private String port;

    @JsonProperty("pid")
    @Column(name = "pid", tag = true)
    private String pid;

    @JsonProperty("node_type")
    @Column(name = "node_type", tag = true)
    private String nodeType;

    @JsonProperty("create_time")
    @Column(name = "create_time")
    private Long createTime;

    @JsonIgnore
    public String getInstanceName() {
        return host + ":" + port;
    }

    @JsonIgnore
    public String getIpPort() {
        return ip + ":" + port;
    }

    @Override
    public Map<String, String> getTags() {
        Map<String, String> tags = Maps.newHashMap();
        tags.put("host", this.getHost());
        tags.put("ip", this.getIp());
        tags.put("port", String.valueOf(this.getPort()));
        tags.put("pid", String.valueOf(this.getPid()));
        tags.put("node_type", String.valueOf(this.getNodeType()));
        return tags;
    }

    @Override
    public Map<String, Object> getFields() {
        Map<String, Object> fields = Maps.newHashMap();
        fields.put("create_time", this.getCreateTime());
        return fields;
    }

    @Override
    public String getTable() {
        return "None";
    }
}
