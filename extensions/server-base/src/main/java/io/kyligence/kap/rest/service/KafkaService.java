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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.kylin.common.util.StreamingMessageRow;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.kafka.StreamingParser;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.source.kafka.CollectKafkaStats;

import javax.annotation.Nullable;

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
        List<String[]> samples = convertMessagesToSamples(identity, prj, messages);
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

    List<String[]> convertMessagesToSamples(String identity, String project, List<String> messages) throws IOException {
        if (0 == messages.size())
            return null;
        KafkaConfig kafkaConfig = getKafkaManager().getKafkaConfig(identity);
        TableDesc tableDesc = getTableManager().getTableDesc(identity, project);
        List<TblColRef> columns = Lists.transform(Arrays.asList(tableDesc.getColumns()), new Function<ColumnDesc, TblColRef>() {
            @Nullable
            @Override
            public TblColRef apply(ColumnDesc input) {
                return input.getRef();
            }
        });
        StreamingParser streamingParser;
        try {
            streamingParser = StreamingParser.getStreamingParser(kafkaConfig.getParserName(), kafkaConfig.getParserProperties(), columns);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(e);
        }
        List<StreamingMessageRow>  streamingMessageRowLists = new ArrayList<>();
        for (String message: messages) {
            ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());
            streamingMessageRowLists.add(streamingParser.parse(buffer).get(0));
        }
        List<String[]> samples = new ArrayList<>();
        int numOfRows = streamingMessageRowLists.size();
        int numOfColumns = streamingMessageRowLists.get(0).getData().size();
        for (int j = 0; j < numOfColumns; j++) {
            String[] columnContent = new String[numOfRows];
            for (int i = 0; i < numOfRows; i++) {
                columnContent[i] = streamingMessageRowLists.get(i).getData().get(j);
            }
            samples.add(columnContent);
        }
        return samples;
    }
}
