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

package org.apache.kylin.query.enumerator;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class OLAPQuery extends AbstractEnumerable<Object[]> implements Enumerable<Object[]> {

    private static final Logger logger = LoggerFactory.getLogger(OLAPQuery.class);

    public enum EnumeratorTypeEnum {
        SIMPLE_AGGREGATION, //probing query like select min(2) from table
        OLAP, //finish query with Cube or II, or a combination of both
        HIVE //using hive
    }

    private final DataContext optiqContext;
    private final EnumeratorTypeEnum type;
    private final int contextId;

    public OLAPQuery(DataContext optiqContext, EnumeratorTypeEnum type, int ctxId) {
        this.optiqContext = optiqContext;
        this.type = type;
        this.contextId = ctxId;
    }

    public OLAPQuery(EnumeratorTypeEnum type, int ctxSeq) {
        this(null, type, ctxSeq);
    }

    public Enumerator<Object[]> enumerator() {
        if (BackdoorToggles.getPrepareOnly())
            return new EmptyEnumerator();
        OLAPContext olapContext = OLAPContext.getThreadLocalContextById(contextId);
        switch (type) {
        case SIMPLE_AGGREGATION:
            return new SingleRowEnumerator();
        case OLAP:
            return new OLAPEnumerator(olapContext, optiqContext);
        case HIVE:
            return new HiveEnumerator(olapContext);
        default:
            throw new IllegalArgumentException("Wrong type " + type + "!");
        }
    }

    public static class EmptyEnumerator implements Enumerator<Object[]> {
        
        public EmptyEnumerator() {
            logger.debug("Using empty enumerator");
        }

        @Override
        public void close() {
        }

        @Override
        public Object[] current() {
            return null;
        }

        @Override
        public boolean moveNext() {
            return false;
        }

        @Override
        public void reset() {
        }
    }

    private abstract static class RowCountEnumerator implements Enumerator<Object[]> {
        private int currentRowCount = 0;
        protected int totalRowCount = 1;

        public RowCountEnumerator() {
            logger.debug("Using ColumnCount enumerator");
        }

         @Override
        public void close() {
        }

         @Override
        public Object[] current() {
            return currentRowCount == totalRowCount ? null : new Object[0];
        }

         @Override
        public boolean moveNext() {
            if (currentRowCount == totalRowCount) {
                return false;
            }
            currentRowCount++;
            return true;
        }

         @Override
        public void reset() {
            currentRowCount = 0;
        }
    }

     private static class SingleRowEnumerator extends RowCountEnumerator {

         public SingleRowEnumerator() {
            super();
            logger.debug("Using SingleRow enumerator");
            totalRowCount = 1;
        }
    }
}
