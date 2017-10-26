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

package io.kyligence.kap.query.util;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.query.util.QueryUtil.IQueryTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.security.HackSelectStarWithColumnACL;
import io.kyligence.kap.query.security.RowFilter;

public class KapQueryUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(KapQueryUtil.class);

    private static List<IQueryTransformer> queryTransformers;

    public static String massageComputedColumn(String ccExpr, String project, String defaultSchema) {
        String transformedCC = ccExpr;

        if (queryTransformers == null) {
            queryTransformers = Lists.newArrayList();
            String[] classes = KylinConfig.getInstanceFromEnv().getQueryTransformers();
            for (String clz : classes) {
                try {
                    IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);
                    if (t instanceof ConvertToComputedColumn || 
                            t instanceof RowFilter ||
                            t instanceof HackSelectStarWithColumnACL) {
                        continue;
                    }
                    queryTransformers.add(t);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to init query transformer", e);
                }
            }
        }

        try {
            for (IQueryTransformer t : queryTransformers) {
                transformedCC = t.transform(transformedCC, project, defaultSchema);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to massage computed column expression {}", ccExpr, e);
            return ccExpr;
        }

        return transformedCC;
    }
}
