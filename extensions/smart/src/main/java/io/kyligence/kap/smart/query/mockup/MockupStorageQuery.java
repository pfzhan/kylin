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

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;

import io.kyligence.kap.smart.query.QueryRecord;

public class MockupStorageQuery extends GTCubeStorageQueryBase {
    private static ThreadLocal<Properties> parameters = new ThreadLocal<>();

    public static final String CUBOID_TRANSLATION = "cuboid.translate";

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

        QueryRecord record = MockupQueryExecutor.getCurrentRecord();
        record.setCubeInstance(cubeInstance);
        record.setGtRequest(request);
        record.setStorageContext(context);

        return ITupleIterator.EMPTY_TUPLE_ITERATOR;
    }

    @Override
    protected String getGTStorage() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Cuboid findCuboid(CubeInstance cube, Set<TblColRef> dimensionsD, Set<FunctionDesc> metrics) {
        long cuboidId = Cuboid.identifyCuboidId(cubeDesc, dimensionsD, metrics);
        String cuboidTrans = getThreadLocalParameter(CUBOID_TRANSLATION);
        if (cuboidTrans != null && cuboidTrans.equals("true")) {
            return Cuboid.findById(cube, cuboidId);
        } else {
            return new Cuboid(cubeDesc, cuboidId, cuboidId);
        }
    }

    @Override
    protected LookupStringTable getLookupStringTableForDerived(TblColRef derived, CubeDesc.DeriveInfo hostInfo) {
        JoinDesc join = hostInfo.join;
        String tableName = join.getPKSide().getTableIdentity();
        String[] pkCols = join.getPrimaryKey();

        try {
            TableDesc tableDesc = TableMetadataManager.getInstance(cubeInstance.getConfig()).getTableDesc(tableName,
                    cubeInstance.getProject());
            return new LookupStringTable(tableDesc, pkCols, new IReadableTable() {
                @Override
                public TableReader getReader() throws IOException {
                    return new TableReader() {
                        @Override
                        public boolean next() throws IOException {
                            return false;
                        }

                        @Override
                        public String[] getRow() {
                            return new String[0];
                        }

                        @Override
                        public void close() throws IOException {

                        }
                    };
                }

                @Override
                public TableSignature getSignature() throws IOException {
                    return null;
                }

                @Override
                public boolean exists() throws IOException {
                    return false;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
