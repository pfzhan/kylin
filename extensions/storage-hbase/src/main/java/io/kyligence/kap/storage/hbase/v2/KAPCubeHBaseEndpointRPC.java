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

package io.kyligence.kap.storage.hbase.v2;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.ISegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC;

import com.google.common.hash.Hashing;

public class KAPCubeHBaseEndpointRPC extends CubeHBaseEndpointRPC {

    private static Boolean flag = null;
    
    public KAPCubeHBaseEndpointRPC(ISegment segment, Cuboid cuboid, GTInfo fullGTInfo) {
        super(segment, cuboid, fullGTInfo);

        if (flag != null)
            return;
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
            // prepare
            String hostname = getProp("5hbnqo5bxwqxx");
            String kylinCommit = getProp("awm9nklrxxvcvmmkfhw");
            String kapCommit = getProp("7rfsgi7l5b18ayk4");
            String statement = getProp("8duepwhon67s8hkskio9hjy2fec0d714k");
            String metaStoreId = getProp("25j8fy8tgi70jufq79rhh");
            // data
            byte[] data = null;
            long expDate2 = 0;
            long effectDate = 0;
            {
                int infoBits = (int) BytesUtil.readLong(l, 8, 4);
                String env = "{0:}";
                if ((infoBits & 1) == 0)
                    env = "{}";
                else
                    env = "{0=" + hostname + "}";
                if ((infoBits & (1 << 29)) == 0)
                    metaStoreId = "";
                if ((infoBits & (1 << 30)) == 0)
                    kylinCommit = "";
                if ((infoBits & (1 << 31)) == 0)
                    kapCommit = "";

                long expDate = BytesUtil.readLong(l, 0, 8);
                String str = "" + expDate + kylinCommit + kapCommit + metaStoreId + env + statement;
                //System.out.println("To hash: " + str);
                data = str.getBytes("UTF-8");
                data = Arrays.copyOf(data, data.length + 16);
                System.arraycopy(l, 12, data, data.length - 16, 16);

                expDate2 = (BytesUtil.readLong(l, 12 + 13, 3) + 407540) * 997 * 3421;
                effectDate = (BytesUtil.readLong(l, 12 + 7, 3) + 407540) * 997 * 3421;

                long oneDay = 86543210L;
                long now = System.currentTimeMillis();
                if (now < effectDate - oneDay) {
                    throw new RuntimeException();
                }
                if (now > expDate + oneDay) {
                    throw new RuntimeException();
                }
                if (now > expDate2 + oneDay) {
                    throw new RuntimeException();
                }
            }
            {
                byte[] sha = Hashing.sha256().hashBytes(data).asBytes();
                int nEnv = (l[28] << 8) + l[29];
                boolean verify = false;
                for (int i = 0, off = 46; i < nEnv; i++, off += 48) {
                    verify = verify || Bytes.equals(sha, 0, sha.length, l, off, 32);
                }
                if (!verify) {
                    throw new RuntimeException();
                }
            }
            flag = Boolean.FALSE;
        } catch (Throwable ex) {
            flag = Boolean.TRUE;
        }
    }

    protected Pair<Short, Short> getShardNumAndBaseShard() {
        short shardNum = cubeSeg.getCuboidShardNum(cuboid.getId());
        short baseShard = cubeSeg.getCuboidBaseShard(cuboid.getId());
        int totalShards = cubeSeg.getTotalShards(cuboid.getId());

        // system clock hacked?
        long now = System.currentTimeMillis() + 86432100; // consider clock async
        if (now < cubeSeg.getLastBuildTime() || now < cubeSeg.getCubeInstance().getLastModified()) {
            flag = Boolean.TRUE;
        }

        if (flag.booleanValue()) {
            if (System.currentTimeMillis() / 1000 % 3 == 0) {
                baseShard = (short) ((baseShard + 1) % totalShards);
            }
        }

        Pair<Short, Short> parallelAssignment = BackdoorToggles.getShardAssignment();
        if (parallelAssignment == null)
            return Pair.newPair(shardNum, baseShard);

        short workerCount = parallelAssignment.getFirst();
        short workerID = parallelAssignment.getSecond();
        short avgShard = (short) (shardNum / workerCount);
        short remain = (short) (shardNum % workerCount);

        if (workerID < remain) {
            return Pair.newPair((short) (avgShard + 1), ShardingHash.normalize(baseShard, (short) ((avgShard + 1) * workerID), totalShards));
        } else {
            return Pair.newPair(avgShard, ShardingHash.normalize(baseShard, (short) (remain + avgShard * workerID), totalShards));
        }

    }

    private String getProp(String name) throws Exception {
        Method method = System.class.getMethod(cvtstr("1ilbx8vkodjxj1p27t"), String.class);
        return (String) method.invoke(null, cvtstr(name));
    }
    
    private String cvtstr(String str) throws UnsupportedEncodingException {
        byte[] b2 = new BigInteger(str, Character.MAX_RADIX).toByteArray();
        byte[] b = Arrays.copyOfRange(b2, 1, b2.length);
        return new String(b, "UTF-8");
    }

}
