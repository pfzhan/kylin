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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.rest.response.ColumnarResponse;
import io.kyligence.kap.smart.cube.CubeOptimizeLogManager;
import io.kyligence.kap.storage.parquet.steps.ColumnarStorageUtils;

@Component("kapCubeService")
public class KapCubeService extends BasicService implements InitializingBean {
    public static final char[] VALID_CUBENAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();
    private static final Logger logger = LoggerFactory.getLogger(KapCubeService.class);

    protected Cache<String, ColumnarResponse> columnarInfoCache = CacheBuilder.newBuilder().build();
    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    AclEvaluate aclEvaluate;

    private ColumnarResponse getColumnarInfo(String segStoragePath, CubeSegment segment) throws IOException {
        final KapConfig kapConfig = KapConfig.wrap(segment.getConfig());
        String key = segment.getCubeInstance().getName() + "/" + segment.getUuid();
        ColumnarResponse response = columnarInfoCache.getIfPresent(key);
        if (response != null) {
            return response;
        }

        logger.debug("Loading TableIndex info " + segment + ", " + segStoragePath);
        
        RawTableManager rawTableManager = RawTableManager.getInstance(segment.getConfig());
        RawTableInstance raw = rawTableManager.getAccompanyRawTable(segment.getCubeInstance());

        ColumnarResponse columnarResp = new ColumnarResponse();
        columnarResp.setDateRangeStart(segment.getTSRange().start.v);
        columnarResp.setDateRangeEnd(segment.getTSRange().end.v);

        FileSystem fs = new Path(segStoragePath).getFileSystem(HadoopUtil.getCurrentConfiguration());
        if (fs.exists(new Path(segStoragePath))) {
            ContentSummary cs = fs.getContentSummary(new Path(segStoragePath));
            columnarResp.setFileCount(cs.getFileCount());
        } else {
            columnarResp.setFileCount(0);
        }
        columnarResp.setStorageSize(segment.getSizeKB() * 1024);
        columnarResp.setSegmentName(segment.getName());
        columnarResp.setSegmentUUID(segment.getUuid());
        columnarResp.setSegmentPath(segStoragePath);

        if (raw != null) {
            List<RawTableSegment> rawSegs = rawTableManager.getRawtableSegmentByTSRange(raw, segment.getTSRange());
            if (rawSegs.size() != 0) {
                Preconditions.checkArgument(rawSegs.size() == 1);
                String rawSegmentDir;
                RawTableSegment rawSegment = rawSegs.get(0);
                if (kapConfig.isParquetSeparateFsEnabled()) {
                    rawSegmentDir = ColumnarStorageUtils.getLocalSegmentDir(
                            rawSegment.getConfig(), KapConfig.wrap(segment.getConfig()).getParquetFileSystem(),
                            rawSegment.getRawTableInstance(), rawSegment);
                } else {
                    rawSegmentDir = getRawParquetFolderPath(rawSegs.get(0));
                }
                columnarResp.setRawTableSegmentPath(rawSegmentDir);

                if (fs.exists(new Path(rawSegmentDir))) {
                    ContentSummary cs = fs.getContentSummary(new Path(rawSegmentDir));
                    columnarResp.setRawTableFileCount(cs.getFileCount());
                    // FIXME: We should store correct size info in segment metadata
                    columnarResp.setRawTableStorageSize(cs.getLength());
                } else {
                    columnarResp.setRawTableFileCount(0);
                    columnarResp.setRawTableStorageSize(0L);
                }
            }
        }

        columnarInfoCache.put(key, columnarResp);
        return columnarResp;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public CubeInstance cubeClone(CubeInstance oldCube, String newCubeName, ProjectInstance project)
            throws IOException {
        Message msg = MsgPicker.getMsg();
        if (oldCube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException(String.format(msg.getCLONE_BROKEN_CUBE(), oldCube.getName()));
        }
        if (!StringUtils.containsOnly(newCubeName, VALID_CUBENAME)) {
            logger.info("Invalid Cube name {}, only letters, numbers and underline supported.", newCubeName);
            throw new BadRequestException(String.format(msg.getINVALID_CUBE_NAME(), newCubeName));
        }

        CubeDesc cubeDesc = oldCube.getDescriptor();
        CubeDesc newCubeDesc = CubeDesc.getCopyOf(cubeDesc);

        newCubeDesc.setName(newCubeName);

        CubeInstance newCube = cubeService.createCubeAndDesc(project, newCubeDesc);

        //reload to avoid shallow clone
        cubeService.getCubeDescManager().reloadCubeDescLocal(newCubeName);

        return newCube;
    }

    public List<ColumnarResponse> getAllColumnarInfo(CubeInstance cube) {
        aclEvaluate.hasProjectReadPermission(cube.getProjectInstance());
        List<ColumnarResponse> columnar = new ArrayList<>();
        final KylinConfig config = cube.getConfig();
        final KapConfig kapConfig = KapConfig.wrap(config);
        for (CubeSegment segment : cube.getSegments()) {
            String storagePath;
            if (kapConfig.isParquetSeparateFsEnabled()) {
                storagePath = ColumnarStorageUtils.getLocalSegmentDir(config, kapConfig.getParquetFileSystem(), cube, segment);
            } else {
                storagePath = ColumnarStorageUtils.getSegmentDir(config, cube, segment);
            }

            ColumnarResponse info;
            try {
                info = getColumnarInfo(storagePath, segment);
            } catch (IOException ex) {
                logger.error("Can't get columnar info, cube {}, segment {}:", cube, segment);
                logger.error("{}", ex);
                continue;
            }

            columnar.add(info);
        }

        return columnar;
    }

    protected String getRawParquetFolderPath(RawTableSegment rawSegment) {
        return new StringBuffer(KapConfig.wrap(rawSegment.getConfig()).getParquetStoragePath())
                .append(rawSegment.getRawTableInstance().getUuid()).append("/").append(rawSegment.getUuid()).append("/")
                .append("RawTable/").toString();
    }

    public void deleteCubeOptLog(String cubeName) throws IOException {
        CubeOptimizeLogManager cubeOptManager = CubeOptimizeLogManager.getInstance(getConfig());
        if (cubeOptManager.getCubeOptimizeLog(cubeName) != null) {
            cubeOptManager.removeCubeOptimizeLog(cubeName);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Broadcaster.getInstance(getConfig()).registerStaticListener(new ColumnarInfoSyncListener(), "cube");
    }

    private class ColumnarInfoSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            columnarInfoCache.invalidateAll();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String cubeName = cacheKey;
            String keyPrefix = cubeName + "/";
            for (String k : columnarInfoCache.asMap().keySet()) {
                if (k.startsWith(keyPrefix))
                    columnarInfoCache.invalidate(k);
            }
        }
    }

}
