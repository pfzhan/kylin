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

package io.kyligence.kap.smart;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSmartMasterRetryTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void transactionExceptionInNSmartMaster() throws ExecutionException, InterruptedException {
        String[] sqlArray1 = { "select item_count, sum(price) from test_kylin_fact group by item_count" };
        String[] sqlArray2 = { "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name" };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<AbstractContext> submit1 = executorService.submit(() -> {
            Thread.currentThread().setName("First thread");
            return NSmartMaster.proposeForAutoMode(getTestConfig(), getProject(), sqlArray1, smartContext -> {
                try {
                    log.info(Thread.currentThread().getName() + " start to sleep");
                    Thread.sleep(10 * 1000);
                    log.info(Thread.currentThread().getName() + " sleep finished");
                } catch (InterruptedException e) {
                    log.info("{} interrupted exception", Thread.currentThread().getName(), e);
                }
            });
        });

        Future<AbstractContext> submit2 = executorService.submit(() -> {
            Thread.currentThread().setName("Second thread");
            return NSmartMaster.proposeForAutoMode(getTestConfig(), getProject(), sqlArray2, null);
        });

        AbstractContext context1 = submit1.get();
        AbstractContext context2 = submit2.get();

        NDataModel targetModel1 = context1.getModelContexts().get(0).getTargetModel();
        NDataModel targetModel2 = context2.getModelContexts().get(0).getTargetModel();
        if (targetModel1.getMvcc() == 1) {
            Assert.assertTrue(targetModel1.getLastModified() > targetModel2.getLastModified());
            Assert.assertEquals(2, targetModel1.getEffectiveDimensions().size());
            Assert.assertEquals(0, targetModel2.getMvcc());
            Assert.assertEquals(1, targetModel2.getEffectiveDimensions().size());
        } else {
            Assert.assertEquals(1, targetModel1.getEffectiveDimensions().size());
            Assert.assertEquals(1, targetModel2.getMvcc());
            Assert.assertEquals(2, targetModel2.getEffectiveDimensions().size());
            Assert.assertTrue(targetModel1.getLastModified() < targetModel2.getLastModified());
        }
    }
}
