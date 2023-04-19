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

package org.apache.kylin.rest;

import org.apache.kylin.rest.delegate.JobStatisticsContract;
import org.apache.kylin.rest.delegate.JobStatisticsInvoker;
import org.apache.kylin.rest.delegate.ModelMetadataContract;
import org.apache.kylin.rest.delegate.ModelMetadataInvoker;
import org.apache.kylin.rest.delegate.TableMetadataContract;
import org.apache.kylin.rest.delegate.TableMetadataInvoker;
import org.apache.kylin.rest.delegate.TableSamplingContract;
import org.apache.kylin.rest.delegate.TableSamplingInvoker;
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
        JobStatisticsInvoker.setDelegate(applicationContext.getBean(JobStatisticsContract.class));
        TableMetadataInvoker.setDelegate(applicationContext.getBean(TableMetadataContract.class));
        TableSamplingInvoker.setDelegate(applicationContext.getBean(TableSamplingContract.class));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
