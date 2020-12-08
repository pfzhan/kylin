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

package io.kyligence.kap.rest.broadcaster;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.rest.util.SpringContext;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.common.persistence.transaction.EpochCheckBroadcastNotifier;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import lombok.Getter;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { SpringContext.class})
@ActiveProfiles("testing")
public class BroadcasterTest extends NLocalFileMetadataTestCase {
    Broadcaster broadcaster;
    private ClusterManager clusterManager = new DefaultClusterManager(7070);

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @Before
    public void setUp() {
        createTestMetadata();
        broadcaster = Broadcaster.getInstance(getTestConfig());
        ReflectionTestUtils.setField(broadcaster, "clusterManager", clusterManager);
    }

    @After
    public void teardown() throws IOException {
        staticCleanupTestMetadata();
        broadcaster.close();
    }

    @Test
    public void testAnnounceWithEpochCheckBroadcastNotifier() {
        final LogAppender appender = new LogAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);

        EpochCheckBroadcastNotifier notifier = new EpochCheckBroadcastNotifier();
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);

        final List<LoggingEvent> log = appender.getLog();
        final LoggingEvent firstLogEntry = log.get(0);
        Assert.assertEquals(Level.INFO, firstLogEntry.getLevel());
        Assert.assertEquals("Broadcast to notify.", firstLogEntry.getMessage());

        final LoggingEvent secondLogEntry = log.get(1);
        Assert.assertEquals(Level.WARN, secondLogEntry.getLevel());
        Assert.assertEquals("Failed to notify.", secondLogEntry.getMessage());
    }

    @Test
    public void testBroadcastType() {
        final LogAppender appender = new LogAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);

        EpochCheckBroadcastNotifier notifier = new EpochCheckBroadcastNotifier();
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);

        List<LoggingEvent> log = appender.getLog();
        Assert.assertEquals(2, log.size());

        // whole nodes
        log.clear();
        notifier.setBroadcastScope(BroadcastEventReadyNotifier.BroadcastScopeEnum.WHOLE_NODES);
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);
        log = appender.getLog();
        Assert.assertEquals(2, log.size());

        // all nodes
        log.clear();
        notifier.setBroadcastScope(BroadcastEventReadyNotifier.BroadcastScopeEnum.ALL_NODES);
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);
        log = appender.getLog();
        Assert.assertEquals(2, log.size());

        log.clear();
        notifier.setBroadcastScope(BroadcastEventReadyNotifier.BroadcastScopeEnum.JOB_NODES);
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);
        log = appender.getLog();
        Assert.assertEquals(0, log.size());

        log.clear();
        notifier.setBroadcastScope(BroadcastEventReadyNotifier.BroadcastScopeEnum.QUERY_NODES);
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);
        log = appender.getLog();
        Assert.assertEquals(0, log.size());
    }

    @Test
    public void testAnnounceWithAuditLogBroadcastEventNotifier() {
        final LogAppender appender = new LogAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);


        AuditLogBroadcastEventNotifier notifier = new AuditLogBroadcastEventNotifier();
        broadcaster.announce(new Broadcaster.BroadcastEvent(), notifier);

        final List<LoggingEvent> log = appender.getLog();
        final LoggingEvent firstLogEntry = log.get(0);
        Assert.assertEquals(Level.INFO, firstLogEntry.getLevel());
        Assert.assertEquals("Broadcast to notify.", firstLogEntry.getMessage());

        final LoggingEvent secondLogEntry = log.get(1);
        Assert.assertEquals(Level.WARN, secondLogEntry.getLevel());
        Assert.assertEquals("Failed to notify.", secondLogEntry.getMessage());
    }

    static class LogAppender extends AppenderSkeleton {
        @Getter
        private final List<LoggingEvent> log = Lists.newArrayList();

        @Override
        protected void append(LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    }

}


