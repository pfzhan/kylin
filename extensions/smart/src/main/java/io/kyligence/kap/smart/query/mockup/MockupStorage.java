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

package io.kyligence.kap.smart.query.mockup;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

public class MockupStorage implements IStorage {
    @Override
    public IStorageQuery createQuery(IRealization realization) {
        if (realization.getType().equals(CubeInstance.REALIZATION_TYPE)) {
            CubeInstance cubeInstance = (CubeInstance) realization;

            IStorageQuery ret;
            try {
                ret = new MockupStorageQuery(cubeInstance);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to initialize storage query for " + MockupStorageQuery.class.getName(), e);
            }

            return ret;
        } else {
            throw new IllegalArgumentException("Unknown realization type " + realization.getType());
        }
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        throw new UnsupportedOperationException();
    }

}
