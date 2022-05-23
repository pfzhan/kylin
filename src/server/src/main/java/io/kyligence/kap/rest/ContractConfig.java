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

package io.kyligence.kap.rest;

import io.kyligence.kap.rest.delegate.JobMetadataContract;
import io.kyligence.kap.rest.delegate.JobMetadataInvoker;
import io.kyligence.kap.rest.delegate.ModelMetadataContract;
import io.kyligence.kap.rest.delegate.ModelMetadataInvoker;
import io.kyligence.kap.rest.delegate.TableMetadataContract;
import io.kyligence.kap.rest.delegate.TableMetadataInvoker;
import io.kyligence.kap.rest.delegate.TableSamplingContract;
import io.kyligence.kap.rest.delegate.TableSamplingInvoker;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ContractConfig implements InitializingBean, ApplicationContextAware {

    ApplicationContext applicationContext = null;

    @Override
    public void afterPropertiesSet() throws Exception {
        ModelMetadataInvoker.setDelegate(applicationContext.getBean(ModelMetadataContract.class));
        JobMetadataInvoker.setDelegate(applicationContext.getBean(JobMetadataContract.class));
        TableMetadataInvoker.setDelegate(applicationContext.getBean(TableMetadataContract.class));
        TableSamplingInvoker.setDelegate(applicationContext.getBean(TableSamplingContract.class));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
