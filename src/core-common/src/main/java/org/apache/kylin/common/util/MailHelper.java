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

package org.apache.kylin.common.util;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.SizeConvertUtil;

public class MailHelper {

    protected static final Logger logger = LoggerFactory.getLogger(MailHelper.class);

    public static List<String> getOverCapacityMailingUsers(KylinConfig kylinConfig) {
        final String[] overCapacityMailingUsers = kylinConfig.getOverCapacityMailingList();
        return getAllNotifyUserList(overCapacityMailingUsers);
    }

    public static List<String> getAllNotifyUserList(String[] notifyUsers) {
        List<String> users = Lists.newArrayList();
        if (null != notifyUsers) {
            Collections.addAll(users, notifyUsers);
        }
        return users;
    }

    public static Pair<String, String> formatNotifications(BasicEmailNotificationContent content) {
        if (content == null) {
            return null;
        }
        String title = content.getEmailTitle();
        String body = content.getEmailBody();
        return Pair.newPair(title, body);
    }

    public static boolean notifyUser(KylinConfig kylinConfig, BasicEmailNotificationContent content,
            List<String> users) {
        try {
            if (users.isEmpty()) {
                logger.debug("no need to send email, user list is empty.");
                return false;
            }
            final Pair<String, String> email = MailHelper.formatNotifications(content);
            return doSendMail(kylinConfig, users, email);
        } catch (Exception e) {
            logger.error("error send email", e);
            return false;
        }
    }

    public static boolean doSendMail(KylinConfig kylinConfig, List<String> users, Pair<String, String> email) {
        if (email == null) {
            logger.warn("no need to send email, content is null");
            return false;
        }
        logger.info("prepare to send email to:{}", users);
        logger.info("notify list:{}", users);
        return new MailService(kylinConfig).sendMail(users, email.getFirst(), email.getSecond());
    }

    public static BasicEmailNotificationContent creatContentForCapacityUsage(Long licenseVolume, Long currentCapacity) {
        BasicEmailNotificationContent content = new BasicEmailNotificationContent();
        content.setIssue("Over capacity threshold");
        content.setTime(LocalDate.now().toString());
        content.setJobType("CHECK_USAGE");
        content.setProject("NULL");

        String readableCurrentCapacity = SizeConvertUtil.getReadableFileSize(currentCapacity);
        String readableLicenseVolume = SizeConvertUtil.getReadableFileSize(licenseVolume);
        double overCapacityThreshold = KylinConfig.getInstanceFromEnv().getOverCapacityThreshold() * 100;
        content.setConclusion(content.CONCLUSION_FOR_OVER_CAPACITY_THRESHOLD
                .replaceAll("\\$\\{volume_used\\}", readableCurrentCapacity)
                .replaceAll("\\$\\{volume_total\\}", readableLicenseVolume)
                .replaceAll("\\$\\{capacity_threshold\\}", BigDecimal.valueOf(overCapacityThreshold).toString()));
        content.setSolution(content.SOLUTION_FOR_OVER_CAPACITY_THRESHOLD);
        return content;
    }

    public static boolean notifyUserForOverCapacity(Long licenseVolume, Long currentCapacity) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<String> users = getOverCapacityMailingUsers(kylinConfig);
        return notifyUser(kylinConfig, creatContentForCapacityUsage(licenseVolume, currentCapacity), users);
    }
}
