/**
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

package io.kyligence.kap.storage.parquet.format.filter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.filter.UDF.MassInValueProvider;
import org.apache.kylin.metadata.filter.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Sets;

public class MassInValueProviderImpl implements MassInValueProvider {
    public static final Logger logger = LoggerFactory.getLogger(MassInValueProviderImpl.class);

    private final static Cache<String, Pair<Long, Set<ByteArray>>> hdfs_caches = CacheBuilder.newBuilder().maximumSize(3).removalListener(new RemovalListener<Object, Object>() {
        @Override
        public void onRemoval(RemovalNotification<Object, Object> notification) {
            logger.debug(String.valueOf(notification.getCause()));
        }
    }).build();

    private Set<ByteArray> ret = Sets.newHashSet();

    public MassInValueProviderImpl(Functions.FilterTableType filterTableType, String filterResourceIdentifier, DimensionEncoding encoding) {

        if (filterTableType == Functions.FilterTableType.HDFS) {

            logger.info("Start to load HDFS filter table from " + filterResourceIdentifier);
            Stopwatch stopwatch = new Stopwatch().start();

            FileSystem fileSystem = null;
            try {
                synchronized (hdfs_caches) {

                    fileSystem = HadoopUtil.getFileSystem(filterResourceIdentifier);

                    long modificationTime = fileSystem.getFileStatus(new Path(filterResourceIdentifier)).getModificationTime();
                    Pair<Long, Set<ByteArray>> cached = hdfs_caches.getIfPresent(filterResourceIdentifier);
                    if (cached != null && cached.getFirst().equals(modificationTime)) {
                        ret = cached.getSecond();
                        logger.info("Load HDFS from cache using " + stopwatch.elapsedMillis() + " millis");
                        return;
                    }

                    InputStream inputStream = fileSystem.open(new Path(filterResourceIdentifier));
                    List<String> lines = IOUtils.readLines(inputStream, Charset.defaultCharset());

                    logger.info("Load HDFS finished after " + stopwatch.elapsedMillis() + " millis");

                    for (String line : lines) {
                        if (StringUtils.isEmpty(line)) {
                            continue;
                        }

                        try {
                            ByteArray byteArray = ByteArray.allocate(encoding.getLengthOfEncoding());
                            encoding.encode(line.getBytes(), line.getBytes().length, byteArray.array(), 0);
                            ret.add(byteArray);
                        } catch (Exception e) {
                            logger.warn("Error when encoding the filter line " + line);
                        }
                    }

                    hdfs_caches.put(filterResourceIdentifier, Pair.newPair(modificationTime, ret));

                    logger.info("Mass In values constructed after " + stopwatch.elapsedMillis() + " millis, containing " + ret.size() + " entries");
                }

            } catch (IOException e) {
                throw new RuntimeException("error when loading the mass in values", e);
            }
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }
    }

    @Override
    public Set<?> getMassInValues() {
        return ret;
    }
}
