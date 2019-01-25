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
package io.kyligence.kap.common.persistence.transaction.mq;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.UnitMessages;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MessageQueue implements Closeable {

    public static MessageQueue getInstance(KylinConfig kylinConfig) {
        if (StringUtils.isEmpty(kylinConfig.getMetadataMQUrl().getScheme())) {
            return null;
        }
        return kylinConfig.getManager(MessageQueue.class);
    }

    private static final Map<String, String> MQ_PROVIDERS = new HashMap<>();
    static {
        MQ_PROVIDERS.put("kafka", "io.kyligence.kap.common.persistence.transaction.kafka.KafkaMessageQueue");
        MQ_PROVIDERS.put("mock", "io.kyligence.kap.common.persistence.transaction.mq.MockedMQ");
    }

    public static final String CONSUMER_THREAD_NAME = "consumer";

    static MessageQueue newInstance(KylinConfig config) {
        String scheme = config.getMetadataMQUrl().getScheme();
        val clazz = MQ_PROVIDERS.get(scheme);
        try {
            val cls = ClassUtil.forName(clazz, MessageQueue.class);
            val instance = cls.getConstructor(KylinConfig.class).newInstance(config);
            return instance;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create event store " + config.getMetadataUrl(), e);
        }
    }

    @Getter
    protected Map<String, String> eventStoreProperties = Maps.newHashMap();

    public abstract EventPublisher getEventPublisher();

    public abstract void startConsumer(Consumer<UnitMessages> consumer);

    public abstract void syncEvents(Consumer<UnitMessages> consumer);

    @Override
    public void close() throws IOException {
        // ignore it;
    }
}
