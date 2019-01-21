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
package io.kyligence.kap.rest.init;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.init.InitialTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;

public class KerberosLoginTask extends InitialTask implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(KerberosLoginTask.class);

    private static Configuration KRB_CONF = new Configuration();

    @Override
    public void execute() {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        try {
            if (kapConfig.isKerberosEnabled()) {
                Preconditions.checkState(KerberosLoginUtil.checkKeyTabIsExist(kapConfig.getKerberosKeytabPath()),
                        "The key tab is not exist : " + kapConfig.getKerberosKeytabPath());
                Preconditions.checkState(KerberosLoginUtil.checkKeyTabIsValid(kapConfig.getKerberosKeytabPath()),
                        "The key tab is invalid : " + kapConfig.getKerberosKeytabPath());
                reInitTGT();
            }
        } catch (IOException e) {
            logger.error("Kerberos login faild.", e);
        }
    }

    private static void loginFIKerberos() throws IOException {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        String zkServerPrincipal = kapConfig.getKerberosZKPrincipal();

        System.setProperty("zookeeper.sasl.client", "true");
        System.setProperty("java.security.auth.login.config",
                KylinConfig.getKylinConfDir() + File.separator + "jaas.conf");
        System.setProperty("java.security.krb5.conf", kapConfig.getKerberosKrb5ConfPath());

        KerberosLoginUtil.setJaasConf("Client", kapConfig.getKerberosPrincipal(), kapConfig.getKerberosKeytabPath());
        KerberosLoginUtil.setZookeeperServerPrincipal(zkServerPrincipal);

        KerberosLoginUtil.login(kapConfig.getKerberosPrincipal(), kapConfig.getKerberosKeytabPath(),
                kapConfig.getKerberosKrb5ConfPath(), KRB_CONF);
    }

    private static void loginStandardKerberos() throws IOException {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();

        UserGroupInformation.loginUserFromKeytab(kapConfig.getKerberosPrincipal(), kapConfig.getKerberosKeytabPath());
        logger.info("Login kerberos success.");
    }

    private void reInitTGT() throws IOException {
        final KapConfig kapConfig = KapConfig.getInstanceFromEnv();

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        logger.info("kinit -kt " + kapConfig.getKerberosKeytabPath() + " "
                                + kapConfig.getKerberosPrincipal());
                        Shell.execCommand("kinit", "-kt", kapConfig.getKerberosKeytabPath(),
                                kapConfig.getKerberosPrincipal());
                        logger.info("Login " + kapConfig.getKerberosPrincipal() + " from keytab: "
                                + kapConfig.getKerberosKeytabPath() + ".");
                        if (kapConfig.getKerberosPlatform().equals("Standard")) {
                            loginStandardKerberos();
                        } else if (kapConfig.getKerberosPlatform().equals("FI")) {
                            loginFIKerberos();
                        }
                        Long krbRefreshTgtTime = kapConfig.getKerberosTicketRefreshInterval();
                        Thread.sleep(krbRefreshTgtTime * 60 * 1000);
                    } catch (InterruptedException ie) {
                        logger.warn("Terminating renewal thread");
                        return;
                    } catch (IOException ie) {
                        logger.warn("Exception encountered while running the"
                                + " renewal command. Aborting renew thread. " + ie);
                        return;
                    }
                }
            }
        });
        t.setDaemon(true);
        t.setName("TGT Reinit for " + UserGroupInformation.getLoginUser().getUserName());
        t.start();

        Thread kerberosMonitor = new Thread(new Runnable() {
            Long krbMonitorTgtTime = kapConfig.getKerberosMonitorInterval();
            @Override
            public void run() {
                while (true) {
                    try {
                        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                        logger.info("current user :" + currentUser);
                        Credentials credentials = currentUser.getCredentials();
                        logger.info("Current use has " + credentials.getAllTokens().size() + " token.");
                        Collection<Token<? extends TokenIdentifier>> allTokens = credentials.getAllTokens();
                        for (Token token : allTokens) {
                            TokenIdentifier tokenIdentifier = token.decodeIdentifier();
                            if (tokenIdentifier instanceof DelegationTokenIdentifier) {
                                logger.info(((DelegationTokenIdentifier) tokenIdentifier).toString());
                            } else {
                                logger.info(tokenIdentifier.toString());
                            }
                        }
                        Thread.sleep(krbMonitorTgtTime * 60 * 1000);
                    } catch (Throwable th) {
                        logger.warn("Error for show delegation :", th);
                        try {
                            Thread.sleep(krbMonitorTgtTime * 60 * 1000);
                        } catch (InterruptedException e) {
                            logger.warn("Sleep interrupted");
                        }
                    }
                }
            }
        });
        kerberosMonitor.setDaemon(true);
        kerberosMonitor
                .setName("Kerberos monitor Thread for user :" + UserGroupInformation.getLoginUser().getUserName());
        kerberosMonitor.start();

    }
}
