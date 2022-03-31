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

package io.kyligence.kap.query;

import io.kyligence.kap.query.engine.SchemaMapExtension;
import io.kyligence.kap.query.engine.SchemaMapExtensionImpl;
import io.kyligence.kap.query.engine.TableColumnAuthExtension;
import io.kyligence.kap.query.engine.TableColumnAuthExtensionImpl;
import io.kyligence.kap.query.engine.mask.QueryResultMasksExtension;
import io.kyligence.kap.query.engine.mask.QueryResultMasksExtensionImpl;


public class QueryExtensionFactoryEnterprise extends QueryExtension.Factory {

    public QueryExtensionFactoryEnterprise() {
        super();
    }

    @Override
    protected QueryResultMasksExtension createQueryResultMasksExtension() {
        return new QueryResultMasksExtensionImpl();
    }

    @Override
    protected SchemaMapExtension createSchemaMapExtension() {
        return new SchemaMapExtensionImpl();
    }

    @Override
    protected TableColumnAuthExtension createTableColumnAuthExtension() {
        return new TableColumnAuthExtensionImpl();
    }

}
