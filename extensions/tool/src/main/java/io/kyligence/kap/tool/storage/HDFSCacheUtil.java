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
package io.kyligence.kap.tool.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;

public class HDFSCacheUtil {

    private static final String CACHE_POOL_PREFIX = "KAP_HDFS_CACHE_POOL";

    private static final long BYTE_IN_KB = 1024;

    private static final long BYTE_IN_MB = BYTE_IN_KB * 1024;

    private static final long BYTE_IN_GB = BYTE_IN_MB * 1024;

    private static final Logger logger = LoggerFactory.getLogger(HDFSCacheUtil.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: HDFSCacheUtil cache  CUBE_NAME");
            System.out.println("Usage: HDFSCacheUtil remove_cache  CUBE_NAME");
            System.out.println("Usage: HDFSCacheUtil check  CUBE_NAME");
            System.out.println("Usage: HDFSCacheUtil list ");
            return;
        }
        String cmd = args[0];

        HDFSCacheUtil cacheUtil = new HDFSCacheUtil();
        String cubeName = null;
        switch (cmd) {
        case "cache":
            cubeName = args[1];
            cacheUtil.cache(cubeName);
            break;
        case "remove_cache":
            cubeName = args[1];
            cacheUtil.removeCache(cubeName);
            break;
        case "check":
            cubeName = args[1];
            if (cacheUtil.existPool(cubeName)) {
                System.out.println("Pool exist");
                cacheUtil.dumpPool(cacheUtil.getDfs(), cacheUtil.getPoolName(cubeName));
            } else {
                System.out.println("Pool not exist");
            }
            break;
        case "list":
            cacheUtil.list(cacheUtil.getDfs());
            break;
        default:
            throw new Exception("Unrecognized arguments.");
        }
    }

    public void cache(String cubeName) throws IOException {
        DistributedFileSystem dfs = getDfs();
        createPoolByCubeName(cubeName, dfs);
        List<String> dirs = getDirByCubeName(cubeName);
        if (dirs.isEmpty()) {
            throw new IOException("Wrong cubeName");
        } else {
            logger.info("Going to cache files for cube : " + cubeName);
            String poolName = getPoolName(cubeName);
            for (String dir : dirs) {
                cacheFilesRecursivly(new Path(dir), dfs, poolName);
            }
            logger.info("Successfully create hdfs cache pool. Pool name :" + poolName);
            dumpPool(dfs, poolName);
        }
    }

    public void removeCache(String cubeName) throws IOException {
        DistributedFileSystem dfs = getDfs();
        logger.info("Going to remove cache files for cube : " + cubeName);
        removePoolByCubeName(cubeName, dfs);
    }

    public void list(DistributedFileSystem dfs) throws IOException {
        List<String> allPools = new ArrayList<>();
        RemoteIterator<CachePoolEntry> pools = dfs.listCachePools();
        while (pools.hasNext()) {
            CachePoolEntry pool = pools.next();
            if (pool.getInfo().getPoolName().startsWith(CACHE_POOL_PREFIX)) {
                //allPools.add(pool.getInfo().getPoolName());
                String poolName = pool.getInfo().getPoolName();
                poolName.substring(CACHE_POOL_PREFIX.length() + 1);
            }
        }
        logger.info("All the cubes that have been cached : " + allPools.toString());
    }

    public boolean existPool(String cubeName) throws IOException {
        String poolName = getPoolName(cubeName);
        return existPool(getDfs(), poolName);
    }

    public boolean existPool(DistributedFileSystem dfs, String poolName) throws IOException {
        RemoteIterator<CachePoolEntry> pools = dfs.listCachePools();
        while (pools.hasNext()) {
            CachePoolEntry pool = pools.next();
            if (pool.getInfo().getPoolName().equals(poolName)) {
                return true;
            }
        }
        return false;
    }

    public CachePoolEntry getPoolEntry(DistributedFileSystem dfs, String poolName) throws IOException {
        RemoteIterator<CachePoolEntry> pools = dfs.listCachePools();
        while (pools.hasNext()) {
            CachePoolEntry pool = pools.next();
            CachePoolInfo info = pool.getInfo();
            if (info.getPoolName().equals(poolName)) {
                return pool;
            }
        }
        return null;
    }

    public void dumpPool(DistributedFileSystem dfs, String poolName) throws IOException {
        RemoteIterator<CachePoolEntry> pools = dfs.listCachePools();
        while (pools.hasNext()) {
            CachePoolEntry pool = pools.next();
            CachePoolInfo info = pool.getInfo();
            if (info.getPoolName().equals(poolName)) {
                CachePoolStats stats = pool.getStats();
                logger.info("Pool name : {}. Pool stat : {}", poolName, getStatDumpString(stats));
                return;
            }
        }
        logger.info("Pool not found");
    }

    public void listPools(DistributedFileSystem dfs) throws IOException {
        RemoteIterator<CachePoolEntry> pools = dfs.listCachePools();
        while (pools.hasNext()) {
            logger.info("pool name : {} ", pools.next().getInfo().getPoolName());
        }
    }

    protected void createPoolByCubeName(String cubeName, DistributedFileSystem dfs) throws IOException {
        createPool(getPoolName(cubeName), dfs);
    }

    protected void removePoolByCubeName(String cubeName, DistributedFileSystem dfs) throws IOException {
        removePool(getPoolName(cubeName), dfs);
    }

    protected void createPool(String poolName, DistributedFileSystem dfs) throws IOException {
        if (existPool(dfs, poolName)) {
            logger.info("found pool exist. Remove it first");
            removePool(poolName, dfs);
        }
        dfs.addCachePool(new CachePoolInfo(poolName));
    }

    protected void removePool(String poolName, DistributedFileSystem dfs) throws IOException {
        if (existPool(dfs, poolName)) {
            dfs.removeCachePool(poolName);
        } else {
            logger.info("not exist pool : {}. Can not remove.", poolName);
        }
    }

    protected List<String> getDirByCubeName(String cubeName) throws IOException {
        List<String> dirs = new ArrayList<>();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        //cuboid file path
        CubeManager manager = CubeManager.getInstance(kylinConfig);
        CubeInstance cubeInstance = manager.getCube(cubeName);
        if (cubeInstance != null) {
            String uuid = cubeInstance.getUuid();
            String dir = kapConfig.getReadParquetStoragePath() + uuid;
            dirs.add(dir);
            logger.info("found cuboid file path : " + dir);
        }

        //raw table file path
        RawTableManager manager2 = RawTableManager.getInstance(kylinConfig);
        RawTableInstance rawInstance = manager2.getRawTableInstance(cubeName);
        if (rawInstance != null) {
            String uuid2 = rawInstance.getUuid();
            String dir2 = kapConfig.getReadParquetStoragePath() + uuid2;
            dirs.add(dir2);
            logger.info("found raw table file path : " + dir2);
        }
        return dirs;
    }

    protected void cacheFilesRecursivly(Path rootPath, DistributedFileSystem dfs, String poolName) throws IOException {
        FileStatus status = dfs.getFileStatus(rootPath);
        if (status.isFile()) {
            logger.info("Going to cache file : " + rootPath);
            dfs.addCacheDirective(new CacheDirectiveInfo.Builder().setPath(status.getPath()).setPool(poolName).build());
        } else {
            FileStatus[] stats = dfs.listStatus(rootPath);
            for (Path p : FileUtil.stat2Paths(stats)) {
                cacheFilesRecursivly(p, dfs, poolName);
            }
        }
    }

    protected String getPoolName(String cubeName) {
        return CACHE_POOL_PREFIX + "_" + cubeName;
    }

    protected DistributedFileSystem getDfs() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs instanceof DistributedFileSystem) {
            return (DistributedFileSystem) fs;
        }
        throw new IOException("Fail to get DistributedFileSystem");
    }

    private String getStatDumpString(CachePoolStats stats) {
        return formatString(stats.getBytesNeeded(), stats.getBytesCached(), stats.getFilesNeeded(), stats.getFilesCached());
    }

    protected static String formatString(long bytesNeeded, long bytesCached, long fileNeeded, long fileCached) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("BytesNeeded : " + getPorperSizeStr(bytesNeeded) + "\t");
        builder.append("BytesCached : " + getPorperSizeStr(bytesCached) + "\t");
        builder.append("FilesNeeded : " + fileNeeded + "\t");
        builder.append("FilesCached : " + fileCached + "}");
        return builder.toString();
    }

    protected static String getPorperSizeStr(long bytes) {
        double result;
        String format = "%.2f%s";
        if (bytes >= BYTE_IN_GB) {
            result = (bytes * 1.0) / BYTE_IN_GB;
            return String.format(format, result, "GB");
        } else if (bytes >= BYTE_IN_MB) {
            result = (bytes * 1.0) / BYTE_IN_MB;
            return String.format(format, result, "MB");
        } else if (bytes >= BYTE_IN_KB) {
            result = bytes * 1.0 / BYTE_IN_KB;
            return String.format(format, result, "KB");
        } else {
            result = bytes * 1.0;
            return String.format(format, result, "B");
        }
    }
}
