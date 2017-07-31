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

package io.kyligence.kap.query.mockup;

import java.util.Properties;
import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;

public class MockupStorageQuery extends GTCubeStorageQueryBase {
    private static ThreadLocal<Properties> parameters = new ThreadLocal<>();

    public static final String CUBOID_TRANSLATATION = "cuboid.translate";

    public static void clearThreadLocalParameter() {
        parameters.remove();
    }

    public static void setThreadLocalParameter(String key, String val) {
        Properties props = parameters.get();
        if (props == null) {
            props = new Properties();
            parameters.set(props);
        }

        if (val == null) {
            props.remove(key);
        } else {
            props.setProperty(key, val);
        }
    }

    public static String getThreadLocalParameter(String key) {
        if (parameters == null) {
            return null;
        }

        Properties props = parameters.get();
        if (props == null) {
            return null;
        }

        return String.valueOf(props.get(key));
    }

    public MockupStorageQuery(CubeInstance cube) {
        super(cube);
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo tupleInfo) {
        GTCubeStorageQueryRequest request = super.getStorageQueryRequest(context, sqlDigest, tupleInfo);

        AbstractQueryRecorder queryRecorder = AbstractQueryRecorder.CURRENT.get();
        queryRecorder.record(cubeInstance, request, context);

        return ITupleIterator.EMPTY_TUPLE_ITERATOR;
    }

    @Override
    protected String getGTStorage() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Cuboid findCuboid(CubeDesc cubeDesc, Set<TblColRef> dimensionsD, Set<FunctionDesc> metrics) {
        long cuboidId = Cuboid.identifyCuboidId(cubeDesc, dimensionsD, metrics);
        String cuboidTrans = getThreadLocalParameter(CUBOID_TRANSLATATION);
        if (cuboidTrans != null && cuboidTrans.equals("true")) {
            return Cuboid.findById(cubeDesc, cuboidId);
        } else {
            return Cuboid.findForFullCube(cubeDesc, cuboidId);
        }
    }
}
