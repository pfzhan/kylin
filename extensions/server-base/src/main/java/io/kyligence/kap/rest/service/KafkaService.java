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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.source.kafka.CollectKafkaStats;

@Component("kafkaClusterService")
public class KafkaService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    public Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        return CollectKafkaStats.getTopics(kafkaConfig);
    }

    public List<String> getMessages(KafkaConfig kafkaConfig, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        return CollectKafkaStats.getMessages(kafkaConfig);
    }

    public String saveSamplesToStreamingTable(String identity, List<String> messages, String prj) throws IOException {
        aclEvaluate.checkProjectWritePermission(prj);
        List<String[]> samples = convertMessagesToSamples(messages);
        TableExtDesc tableExtDesc = getTableManager().getTableExt(identity, prj);
        tableExtDesc.setSampleRows(samples);
        getTableManager().saveTableExt(tableExtDesc, prj);
        return "OK";
    }

    public List<String> updateSamplesByTableName(String tableName, String prj) throws IOException {
        aclEvaluate.checkProjectWritePermission(prj);
        KafkaConfig kafkaConfig = getKafkaManager().getKafkaConfig(tableName);
        List<String> messages = CollectKafkaStats.getMessages(kafkaConfig);
        saveSamplesToStreamingTable(tableName, messages, prj);
        return messages;
    }

    private List<String[]> convertMessagesToSamples(List<String> messages) throws IOException {
        if (0 == messages.size())
            return null;
        Map<String, List<String>> messageTable = new LinkedHashMap<>();
        for (int i = 0; i < messages.size(); i++) {
            String row = messages.get(i);
            String[] values = row.replace('{', ' ').replace('}', ' ').trim().split(",");
            for (int j = 0; j < values.length; j++) {
                String[] value = values[j].split(":");
                String columnName = value[0].replace("\"", " ").trim();
                String columnValue = value[1].replace("\"", " ").trim();
                if (messageTable.get(columnName) == null) {
                    List<String> columnValues = new ArrayList<>();
                    columnValues.add(columnValue);
                    messageTable.put(columnName, columnValues);
                } else {
                    messageTable.get(columnName).add(columnValue);
                }
            }
        }
        List<String[]> samples = new ArrayList<>();
        for (List<String> values : messageTable.values()) {
            samples.add(values.toArray(new String[values.size()]));
        }
        return samples;
    }
}
