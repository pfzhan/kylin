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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.LoggerFactory;

/**
 * @author xduo
 */
public class MailService {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MailService.class);

    private Boolean enabled = Boolean.TRUE;
    private Boolean starttlsEnabled = Boolean.FALSE;
    private String host;
    private String port;
    private String username;
    private String password;
    private String sender;

    public MailService(KylinConfig config) {
        this(config.isMailEnabled(), config.isStarttlsEnabled(), config.getMailHost(), config.getSmtpPort(), config.getMailUsername(), config.getMailPassword(), config.getMailSender());
    }

    private MailService(boolean enabled, boolean starttlsEnabled, String host, String port, String username, String password, String sender) {
        this.enabled = enabled;
        this.starttlsEnabled = starttlsEnabled;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.sender = sender;

        if (enabled) {
            if (host.isEmpty()) {
                throw new RuntimeException("mail service host is empty");
            }
        }
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content) {
        return sendMail(receivers, subject, content, true);
    }

    /**
     * @param receivers
     * @param subject
     * @param content
     * @return true or false indicating whether the email was delivered successfully
     * @throws IOException
     */
    public boolean sendMail(List<String> receivers, String subject, String content, boolean isHtmlMsg) {

        if (!enabled) {
            logger.info("Email service is disabled; this mail will not be delivered: " + subject);
            logger.info("To enable mail service, set 'kylin.job.notification-enabled=true' in kylin.properties");
            return false;
        }

        Email email = new HtmlEmail();
        email.setHostName(host);
        email.setStartTLSEnabled(starttlsEnabled);
        if (starttlsEnabled) {
            email.setSslSmtpPort(port);
        } else {
            email.setSmtpPort(Integer.valueOf(port));
        }
        
        if (username != null && username.trim().length() > 0) {
            email.setAuthentication(username, password);
        }

        //email.setDebug(true);
        try {
            for (String receiver : receivers) {
                email.addTo(receiver);
            }

            email.setFrom(sender);
            email.setSubject(subject);
            email.setCharset("UTF-8");
            if (isHtmlMsg) {
                ((HtmlEmail) email).setHtmlMsg(content);
            } else {
                ((HtmlEmail) email).setTextMsg(content);
            }
            email.send();
            email.getMailSession();

        } catch (EmailException e) {
            logger.error(e.getLocalizedMessage(), e);
            return false;
        }

        return true;
    }
}
