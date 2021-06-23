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

import java.util.Arrays;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class OLAPEnumerator implements Enumerator<Object[]> {

    private final static Logger logger = LoggerFactory.getLogger(OLAPEnumerator.class);

    private final OLAPContext olapContext;
    private final DataContext optiqContext;
    private Object[] current;
    private ITupleIterator cursor;

    public OLAPEnumerator(OLAPContext olapContext, DataContext optiqContext) {
        this.olapContext = olapContext;
        this.optiqContext = optiqContext;
        this.cursor = null;
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            if (cursor == null) {
                cursor = queryStorage();
            }

            if (!cursor.hasNext()) {
                return false;
            }

            ITuple tuple = cursor.next();
            if (tuple == null) {
                return false;
            }
            convertCurrentRow(tuple);
            return true;
        } catch (Exception e) {
            try {
                if (cursor != null) {
                    cursor.close();
                }
            } catch (Exception ee) {
                logger.info("Error when closing cursor, ignore it", ee);
            }
            throw e;
        }
    }

    private void convertCurrentRow(ITuple tuple) {
        // give calcite a new array every time, see details in KYLIN-2134
        Object[] values = tuple.getAllValues();
        current = Arrays.copyOf(values, values.length);
    }

    @Override
    public void reset() {
        close();
        cursor = queryStorage();
    }

    @Override
    public void close() {
        if (cursor != null)
            cursor.close();
    }

    private ITupleIterator queryStorage() {
        logger.debug("query storage...");
        // bind dynamic variables and update filter info in OLAPContext
        SQLDigest sqlDigest = olapContext.getSQLDigest();

        // query storage engine
        IStorageQuery storageEngine = StorageFactory.createQuery(olapContext.realization);
        ITupleIterator iterator = storageEngine.search(olapContext.storageContext, sqlDigest,
                olapContext.returnTupleInfo);
        if (logger.isDebugEnabled()) {
            logger.debug("return TupleIterator...");
        }

        return iterator;
    }

}
