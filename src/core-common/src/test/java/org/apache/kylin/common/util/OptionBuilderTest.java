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

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.common.util.OptionBuilder;

public class OptionBuilderTest {

    @Test
    public void testBasic() {
        String argName = "dir";
        String opt = "destDir";
        String desc = "specify the dest dir to save the related information";
        String longOpt = "dest_dir";
        char sep = ',';
        Option option = OptionBuilder.getInstance().withArgName(argName).hasArg().isRequired(true).withDescription(desc)
                .withLongOpt(longOpt).withType(OptionBuilder.class).withValueSeparator(sep).create(opt);

        Assert.assertTrue(option.isRequired());
        Assert.assertTrue(option.hasArg());
        Assert.assertEquals(opt, option.getOpt());
        Assert.assertEquals(argName, option.getArgName());
        Assert.assertEquals(desc, option.getDescription());
        Assert.assertEquals(sep, option.getValueSeparator());
        Assert.assertEquals(longOpt, option.getLongOpt());
        Assert.assertEquals(OptionBuilder.class, option.getType());

        Option optionFalse = OptionBuilder.getInstance().withArgName(argName).hasArg().isRequired(false)
                .withDescription(desc).withLongOpt(longOpt).withType(OptionBuilder.class).withValueSeparator(sep)
                .create(opt);
        Assert.assertFalse(optionFalse.isRequired());
    }

    @Test
    public void testConcurrent() {
        Thread t1 = new Thread(() -> {
            while (true) {
                org.apache.commons.cli.OptionBuilder.isRequired(false).create("falseOpt");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("t1 interrupt");
                }
            }
        });

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            t1.start();

            while (true) {
                Option option = org.apache.commons.cli.OptionBuilder.isRequired(true).create("trueOpt");
                if (!option.isRequired()) {
                    // concurrent issue
                    break;
                }
            }
            t1.interrupt();
        });

        Thread t2 = new Thread(() -> {
            while (true) {
                OptionBuilder.getInstance().isRequired(false).create("falseOpt");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("t2 interrupt");
                }
            }
        });

        t2.start();

        for (int i = 0; i < 10000; i++) {
            Option option = OptionBuilder.getInstance().isRequired(true).create("trueOpt");
            Assert.assertTrue(option.isRequired());
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.out.println("t2 interrupt");
            }
        }

        t2.interrupt();
    }

}
