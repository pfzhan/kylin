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

package io.kyligence.kap.storage.parquet.cube.raw;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.gtrecord.RawTableSegmentScanner;
import io.kyligence.kap.gtrecord.SequentialRawTableTupleIterator;
import kap.google.common.collect.Sets;

public class RawTableStorageQuery implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(RawTableStorageQuery.class);

    private RawTableInstance rawTableInstance;
    private RawTableDesc rawTableDesc;

    public RawTableStorageQuery(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
        this.rawTableDesc = rawTableInstance.getRawTableDesc();
    }

    private void hackSelectStar(SQLDigest sqlDigest) {
        if (!sqlDigest.isRawQuery) {
            return;
        }

        // If it's select * from ...,
        // We need to retrieve cube to manually add columns into sqlDigest, so that we have full-columns results as output.
        boolean isSelectAll = sqlDigest.allColumns.isEmpty() || sqlDigest.allColumns.equals(sqlDigest.filterColumns);

        if (!isSelectAll)
            return;

        for (TblColRef col : this.rawTableDesc.getColumns()) {
            if (col.getTable().equals(sqlDigest.factTable)) {
                sqlDigest.allColumns.add(col);
            }
        }
    }
    
    private static String flag = null;
    private static int count = 0;
    private String getProp(String name) throws Exception {
        Method method = System.class.getMethod(cvtstr("1ilbx8vkodjxj1p27t"), String.class);
        return (String) method.invoke(null, cvtstr(name));
    }

    private String cvtstr(String str) throws UnsupportedEncodingException {
        byte[] b2 = new BigInteger(str, Character.MAX_RADIX).toByteArray();
        byte[] b = Arrays.copyOfRange(b2, 1, b2.length);
        return new String(b, "UTF-8");
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {

        hackSelectStar(sqlDigest);

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        enableStorageLimitIfPossible(sqlDigest, sqlDigest.filter, context);
        context.setFinalPushDownLimit(rawTableInstance);


        List<RawTableSegmentScanner> scanners = Lists.newArrayList();
        for (RawTableSegment rawTableSegment : rawTableInstance.getSegments(SegmentStatusEnum.READY)) {
            RawTableSegmentScanner scanner;
            if (rawTableSegment.getCubeSegment().getInputRecords() == 0) {
                if (!skipZeroInputSegment(rawTableSegment)) {
                    logger.warn("raw segment {} input record is 0, " + "it may caused by kylin failed to the job counter " + "as the hadoop history server wasn't running", rawTableSegment);
                } else {
                    logger.warn("raw segment {} input record is 0, skip it ", rawTableSegment);
                    continue;
                }
            }
            
            if (++count % 200 == 0) {
                byte[] pubkey = new byte[] { (byte) 0x30, (byte) 0x82, (byte) 0x1, (byte) 0xb8, (byte) 0x30, (byte) 0x82, (byte) 0x1, (byte) 0x2c, (byte) 0x6, (byte) 0x7, (byte) 0x2a, (byte) 0x86, (byte) 0x48, (byte) 0xce, (byte) 0x38, (byte) 0x4, (byte) 0x1, (byte) 0x30, (byte) 0x82, (byte) 0x1, (byte) 0x1f, (byte) 0x2, (byte) 0x81, (byte) 0x81, (byte) 0x0, (byte) 0xfd, (byte) 0x7f, (byte) 0x53, (byte) 0x81, (byte) 0x1d, (byte) 0x75, (byte) 0x12, (byte) 0x29, (byte) 0x52, (byte) 0xdf, (byte) 0x4a, (byte) 0x9c, (byte) 0x2e, (byte) 0xec, (byte) 0xe4, (byte) 0xe7, (byte) 0xf6, (byte) 0x11, (byte) 0xb7, (byte) 0x52, (byte) 0x3c, (byte) 0xef, (byte) 0x44, (byte) 0x0, (byte) 0xc3, (byte) 0x1e, (byte) 0x3f, (byte) 0x80, (byte) 0xb6, (byte) 0x51, (byte) 0x26, (byte) 0x69, (byte) 0x45, (byte) 0x5d, (byte) 0x40, (byte) 0x22, //
                        (byte) 0x51, (byte) 0xfb, (byte) 0x59, (byte) 0x3d, (byte) 0x8d, (byte) 0x58, (byte) 0xfa, (byte) 0xbf, (byte) 0xc5, (byte) 0xf5, (byte) 0xba, (byte) 0x30, (byte) 0xf6, (byte) 0xcb, (byte) 0x9b, (byte) 0x55, (byte) 0x6c, (byte) 0xd7, (byte) 0x81, (byte) 0x3b, (byte) 0x80, (byte) 0x1d, (byte) 0x34, (byte) 0x6f, (byte) 0xf2, (byte) 0x66, (byte) 0x60, (byte) 0xb7, (byte) 0x6b, (byte) 0x99, (byte) 0x50, (byte) 0xa5, (byte) 0xa4, (byte) 0x9f, (byte) 0x9f, (byte) 0xe8, (byte) 0x4, (byte) 0x7b, (byte) 0x10, (byte) 0x22, (byte) 0xc2, (byte) 0x4f, (byte) 0xbb, (byte) 0xa9, (byte) 0xd7, (byte) 0xfe, (byte) 0xb7, (byte) 0xc6, (byte) 0x1b, (byte) 0xf8, (byte) 0x3b, (byte) 0x57, (byte) 0xe7, (byte) 0xc6, (byte) 0xa8, (byte) 0xa6, (byte) 0x15, (byte) 0xf, (byte) 0x4, (byte) 0xfb, (byte) 0x83, (byte) 0xf6, //
                        (byte) 0xd3, (byte) 0xc5, (byte) 0x1e, (byte) 0xc3, (byte) 0x2, (byte) 0x35, (byte) 0x54, (byte) 0x13, (byte) 0x5a, (byte) 0x16, (byte) 0x91, (byte) 0x32, (byte) 0xf6, (byte) 0x75, (byte) 0xf3, (byte) 0xae, (byte) 0x2b, (byte) 0x61, (byte) 0xd7, (byte) 0x2a, (byte) 0xef, (byte) 0xf2, (byte) 0x22, (byte) 0x3, (byte) 0x19, (byte) 0x9d, (byte) 0xd1, (byte) 0x48, (byte) 0x1, (byte) 0xc7, (byte) 0x2, (byte) 0x15, (byte) 0x0, (byte) 0x97, (byte) 0x60, (byte) 0x50, (byte) 0x8f, (byte) 0x15, (byte) 0x23, (byte) 0xb, (byte) 0xcc, (byte) 0xb2, (byte) 0x92, (byte) 0xb9, (byte) 0x82, (byte) 0xa2, (byte) 0xeb, (byte) 0x84, (byte) 0xb, (byte) 0xf0, (byte) 0x58, (byte) 0x1c, (byte) 0xf5, (byte) 0x2, (byte) 0x81, (byte) 0x81, (byte) 0x0, (byte) 0xf7, (byte) 0xe1, (byte) 0xa0, (byte) 0x85, (byte) 0xd6, //
                        (byte) 0x9b, (byte) 0x3d, (byte) 0xde, (byte) 0xcb, (byte) 0xbc, (byte) 0xab, (byte) 0x5c, (byte) 0x36, (byte) 0xb8, (byte) 0x57, (byte) 0xb9, (byte) 0x79, (byte) 0x94, (byte) 0xaf, (byte) 0xbb, (byte) 0xfa, (byte) 0x3a, (byte) 0xea, (byte) 0x82, (byte) 0xf9, (byte) 0x57, (byte) 0x4c, (byte) 0xb, (byte) 0x3d, (byte) 0x7, (byte) 0x82, (byte) 0x67, (byte) 0x51, (byte) 0x59, (byte) 0x57, (byte) 0x8e, (byte) 0xba, (byte) 0xd4, (byte) 0x59, (byte) 0x4f, (byte) 0xe6, (byte) 0x71, (byte) 0x7, (byte) 0x10, (byte) 0x81, (byte) 0x80, (byte) 0xb4, (byte) 0x49, (byte) 0x16, (byte) 0x71, (byte) 0x23, (byte) 0xe8, (byte) 0x4c, (byte) 0x28, (byte) 0x16, (byte) 0x13, (byte) 0xb7, (byte) 0xcf, (byte) 0x9, (byte) 0x32, (byte) 0x8c, (byte) 0xc8, (byte) 0xa6, (byte) 0xe1, (byte) 0x3c, (byte) 0x16, (byte) 0x7a, //
                        (byte) 0x8b, (byte) 0x54, (byte) 0x7c, (byte) 0x8d, (byte) 0x28, (byte) 0xe0, (byte) 0xa3, (byte) 0xae, (byte) 0x1e, (byte) 0x2b, (byte) 0xb3, (byte) 0xa6, (byte) 0x75, (byte) 0x91, (byte) 0x6e, (byte) 0xa3, (byte) 0x7f, (byte) 0xb, (byte) 0xfa, (byte) 0x21, (byte) 0x35, (byte) 0x62, (byte) 0xf1, (byte) 0xfb, (byte) 0x62, (byte) 0x7a, (byte) 0x1, (byte) 0x24, (byte) 0x3b, (byte) 0xcc, (byte) 0xa4, (byte) 0xf1, (byte) 0xbe, (byte) 0xa8, (byte) 0x51, (byte) 0x90, (byte) 0x89, (byte) 0xa8, (byte) 0x83, (byte) 0xdf, (byte) 0xe1, (byte) 0x5a, (byte) 0xe5, (byte) 0x9f, (byte) 0x6, (byte) 0x92, (byte) 0x8b, (byte) 0x66, (byte) 0x5e, (byte) 0x80, (byte) 0x7b, (byte) 0x55, (byte) 0x25, (byte) 0x64, (byte) 0x1, (byte) 0x4c, (byte) 0x3b, (byte) 0xfe, (byte) 0xcf, (byte) 0x49, (byte) 0x2a, (byte) 0x3, //
                        (byte) 0x81, (byte) 0x85, (byte) 0x0, (byte) 0x2, (byte) 0x81, (byte) 0x81, (byte) 0x0, (byte) 0xc3, (byte) 0x24, (byte) 0x71, (byte) 0xa0, (byte) 0xe, (byte) 0x39, (byte) 0xa3, (byte) 0x82, (byte) 0x51, (byte) 0x42, (byte) 0x4, (byte) 0xcb, (byte) 0x58, (byte) 0xcb, (byte) 0x7b, (byte) 0xe3, (byte) 0xf7, (byte) 0x58, (byte) 0x41, (byte) 0xa5, (byte) 0x81, (byte) 0xe1, (byte) 0x52, (byte) 0x14, (byte) 0x7b, (byte) 0x72, (byte) 0x1a, (byte) 0x7e, (byte) 0xc9, (byte) 0x9c, (byte) 0x7f, (byte) 0x26, (byte) 0xba, (byte) 0x9c, (byte) 0xff, (byte) 0x1b, (byte) 0x98, (byte) 0xfa, (byte) 0x73, (byte) 0x96, (byte) 0x6c, (byte) 0x21, (byte) 0x93, (byte) 0x23, (byte) 0x93, (byte) 0x94, (byte) 0x24, (byte) 0xee, (byte) 0x42, (byte) 0x8f, (byte) 0xef, (byte) 0x34, (byte) 0x69, (byte) 0x46, (byte) 0xd3, //
                        (byte) 0x3e, (byte) 0xce, (byte) 0x83, (byte) 0xd8, (byte) 0xa2, (byte) 0x1c, (byte) 0x57, (byte) 0x28, (byte) 0xa3, (byte) 0xf7, (byte) 0x5e, (byte) 0xba, (byte) 0x60, (byte) 0x1c, (byte) 0xe6, (byte) 0x59, (byte) 0x2, (byte) 0x89, (byte) 0xf3, (byte) 0x82, (byte) 0xfc, (byte) 0xd7, (byte) 0x54, (byte) 0xe2, (byte) 0xe9, (byte) 0x7e, (byte) 0xeb, (byte) 0x5a, (byte) 0x89, (byte) 0xd2, (byte) 0x18, (byte) 0xba, (byte) 0x10, (byte) 0x1a, (byte) 0xf6, (byte) 0x2c, (byte) 0x2b, (byte) 0x82, (byte) 0x96, (byte) 0x4b, (byte) 0x7e, (byte) 0x9e, (byte) 0x41, (byte) 0x5c, (byte) 0xbc, (byte) 0x3b, (byte) 0x23, (byte) 0x19, (byte) 0xc9, (byte) 0xee, (byte) 0x47, (byte) 0x36, (byte) 0x5, (byte) 0xaf, (byte) 0xe5, (byte) 0x8c, (byte) 0x4a, (byte) 0x5f, (byte) 0x3c, (byte) 0x61, (byte) 0xbd, (byte) 0x84, //
                        (byte) 0x6a, (byte) 0x19, (byte) 0x4f, (byte) 0xf2, (byte) 0xd7, (byte) 0xa6, (byte) 0xae, (byte) 0xc2, (byte) 0xc5, (byte) 0x36, (byte) 0xf5 };

                try {
                    // read license
                    byte[] l;
                    {
                        String lstr = getProp("1j74addin9cooh2nad");
                        byte[] ltmp = new BigInteger(lstr, Character.MAX_RADIX).toByteArray();
                        l = Arrays.copyOfRange(ltmp, 1, ltmp.length);
                    }
                    // de-obfuscate
                    {
                        for (int i = 0, j = l.length - 1; i < j; i++, j--) {
                            if ((l[i] & 1) == (l[j] & 1)) {
                                l[i] = (byte) ~l[i];
                                l[j] = (byte) ~l[j];
                            } else {
                                byte t = l[i];
                                l[i] = l[j];
                                l[j] = t;
                            }
                        }
                    }
                    // validate signature
                    int nEnv = (l[28] << 8) + l[29];
                    int pubOff = 30 + 48 * nEnv;
                    PublicKey pub2 = KeyFactory.getInstance("DSA").generatePublic(new X509EncodedKeySpec(pubkey));
                    Signature dsa = Signature.getInstance("SHA1withDSA");
                    dsa.initVerify(pub2);
                    dsa.update(l, 0, pubOff);
                    boolean verify = dsa.verify(l, pubOff + 444, l.length - pubOff - 444);
                    if (!verify) {
                        flag = "UTF-8";
                        return null;
                    }
                    // return infoBits
                    int infoBits = (int) BytesUtil.readLong(l, 8, 4);
                    if ((infoBits & (1 << 28)) == 0)
                        flag = "UTF-8";
                } catch (Throwable ex) {
                    flag = "UTF-8";
                }
            }
            if (flag == "UTF-8")
                return null;

            Set<TblColRef> groups = Sets.newHashSet();
            scanner = new RawTableSegmentScanner(rawTableSegment, dimensions, groups, Collections.<FunctionDesc> emptySet(), sqlDigest.filter, context);
            scanners.add(scanner);
        }
        return new SequentialRawTableTupleIterator(scanners, rawTableInstance, dimensions, metrics, returnTupleInfo, context);
    }

    private void enableStorageLimitIfPossible(SQLDigest sqlDigest, TupleFilter filter, StorageContext context) {
        boolean possible = true;

        boolean isRaw = sqlDigest.isRawQuery;
        if (!isRaw) {
            possible = false;
            logger.info("Storage limit push down it's a non-raw query");
        }

        boolean goodFilter = filter == null || TupleFilter.isEvaluableRecursively(filter);
        if (!goodFilter) {
            possible = false;
            logger.info("Storage limit push down is impossible because the filter is unevaluatable");
        }

        boolean goodSort = !context.hasSort();
        if (!goodSort) {
            possible = false;
            logger.info("Storage limit push down is impossible because the query has order by");
        }

        if (possible) {
            logger.info("Enable limit " + context.getLimit());
            context.enableLimit();
        }
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (TblColRef column : sqlDigest.allColumns) {
            dimensions.add(column);
        }
    }

    protected boolean skipZeroInputSegment(RawTableSegment segment) {
        return true;
    }

}
