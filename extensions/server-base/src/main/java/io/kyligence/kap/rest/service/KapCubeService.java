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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.mp.MPCubeManager;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.metadata.model.IKapStorageAware;
import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
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
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    AclEvaluate aclEvaluate;

    public MPCubeManager getMPCubeManager() {
        return MPCubeManager.getInstance(getConfig());
    }

    private ColumnarResponse getColumnarInfo(String segStoragePath, CubeSegment segment) throws IOException {
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
                String rawSegmentDir = ColumnarStorageUtils.getReadSegmentDir(rawSegs.get(0));
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

    protected String getRawParquetFolderPath(RawTableSegment rawSegment) {
        return new StringBuffer(KapConfig.wrap(rawSegment.getConfig()).getReadParquetStoragePath())
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

    public JobInstance mergeSegment(String cubeName, List<String> selectedSegments, boolean force) throws IOException {
        CubeInstance cube = getCubeManager().getCube(cubeName);
        Segments<CubeSegment> segs = new Segments<>();
        for (CubeSegment s : cube.getSegments()) {
            if (selectedSegments.contains(s.getName())) {
                if (s.getStatus() != SegmentStatusEnum.READY)
                    throw new IllegalArgumentException("Segment " + s + " is not in READY state and cannot be merged");
                segs.add(s);
            }
        }

        if (segs.size() < 2)
            throw new IllegalArgumentException("" + segs.size() + " segment selected, cannot merge");

        Collections.sort(segs);
        CubeSegment first = segs.get(0);
        CubeSegment last = segs.get(segs.size() - 1);

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();

        if (first.isOffsetCube()) {
            SegmentRange range = new SegmentRange(first.getSegRange().start, last.getSegRange().end);
            return jobService.submitJob(cube, null, range, null, null, CubeBuildTypeEnum.MERGE, force, submitter);
        } else {
            TSRange range = new TSRange(first.getTSRange().start.v, last.getTSRange().end.v);
            return jobService.submitJob(cube, range, null, null, null, CubeBuildTypeEnum.MERGE, force, submitter);
        }
    }

    public List<JobInstance> refreshSegments(String cubeName, List<String> selectedSegments) throws IOException {
        List<JobInstance> ret = new ArrayList<>();
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();

        CubeInstance cube = getCubeManager().getCube(cubeName);
        for (CubeSegment s : cube.getSegments()) {
            if (selectedSegments.contains(s.getName())) {
                JobInstance job;
                if (s.isOffsetCube()) {
                    job = jobService.submitJob(cube, null, s.getSegRange(), null, null, CubeBuildTypeEnum.REFRESH, true,
                            submitter);
                } else {
                    job = jobService.submitJob(cube, s.getTSRange(), null, null, null, CubeBuildTypeEnum.REFRESH, true,
                            submitter);
                }
                ret.add(job);
            }
        }
        return ret;
    }

    public void dropSegments(String cubeName, List<String> selectedSegments) throws IOException {
        CubeInstance cube = getCubeManager().getCube(cubeName);
        for (String seg : selectedSegments) {
            cubeService.deleteSegment(cube, seg);
        }
    }

    public String getProjectOfCube(CubeInstance cube) {
        return projectService.getProjectOfCube(cube.getName());
    }

    public long computeSegmentsStorage(CubeInstance cube, List<CubeSegment> segments) {
        // TODO The logic of retrieving storage info should be extracted into Storage interface.
        // TODO The info should be collected at the end of the cube build, really.

        // It is intended to store info directly in CubeSegment.additionalInfo, such that
        // 1) CubeSegment becomes a natural cache; 2) the info become persistent on next cube save.

        long totalStorageSize = 0;

        int storageType = cube.getStorageType();
        if (storageType == IKapStorageAware.ID_SHARDED_PARQUET || storageType == IKapStorageAware.ID_SPLICE_PARQUET) {
            for (CubeSegment seg : segments) {
                if (seg.getStatus() != SegmentStatusEnum.READY && seg.getStatus() != SegmentStatusEnum.READY_PENDING)
                    continue;

                Map<String, String> addInfo = seg.getAdditionalInfo();
                if (addInfo.containsKey("storageType")) {
                    totalStorageSize += getLong(addInfo, "storageSizeBytes")
                            + getLong(addInfo, "tableIndexStorageSizeBytes");
                    continue;
                }

                String storagePath = ColumnarStorageUtils.getReadSegmentDir(seg);

                ColumnarResponse info;
                try {
                    info = getColumnarInfo(storagePath, seg);
                } catch (IOException ex) {
                    logger.error("Can't get columnar info, " + cube + ", " + seg + ":", ex);
                    continue;
                }

                addInfo.put("storageType", "" + storageType);
                addInfo.put("segmentPath", info.getSegmentPath());
                addInfo.put("storageFileCount", "" + info.getFileCount());
                addInfo.put("storageSizeBytes", "" + info.getStorageSize());
                addInfo.put("tableIndexSegmentPath", info.getRawTableSegmentPath());
                addInfo.put("tableIndexFileCount", "" + info.getRawTableFileCount());
                addInfo.put("tableIndexStorageSizeBytes", "" + info.getRawTableStorageSize());

                totalStorageSize += info.getStorageSize() + info.getRawTableStorageSize();
            }
        } else {
            for (CubeSegment seg : segments) {
                if (seg.getStatus() != SegmentStatusEnum.READY && seg.getStatus() != SegmentStatusEnum.READY_PENDING)
                    continue;

                Map<String, String> addInfo = seg.getAdditionalInfo();
                if (addInfo.containsKey("storageType")) {
                    totalStorageSize += getLong(addInfo, "storageSizeBytes");
                    continue;
                }

                HBaseResponse info;
                try {
                    info = cubeService.getHTableInfo(cube.getName(), seg.getStorageLocationIdentifier());
                } catch (IOException e) {
                    logger.error("Failed to calculate size of HTable '" + seg.getStorageLocationIdentifier() + "'.", e);
                    continue;
                }

                addInfo.put("storageType", "" + storageType);
                addInfo.put("storageSizeBytes", "" + fixHBaseSmallTableSize(info, seg));
                addInfo.put("hbaseTableName", seg.getStorageLocationIdentifier());
                addInfo.put("hbaseRegionCount", "" + info.getRegionCount());

                totalStorageSize += info.getTableSize();
            }
        }

        return totalStorageSize / 1024;
    }

    // HBase API returns table size in MB and that could be 0 for small cube under 1 MB
    private long fixHBaseSmallTableSize(HBaseResponse info, CubeSegment seg) {
        long bytes = info.getTableSize();
        if (bytes == 0)
            bytes = seg.getSizeKB() * 1024; // use the size tracked during build time instead
        return bytes;
    }

    private long getLong(Map<String, String> addInfo, String key) {
        String str = addInfo.get(key);
        if (str == null)
            return 0;
        else
            return Long.parseLong(str);
    }

    public void computeSegmentsOperativeFlags(CubeInstance cube, List<CubeSegment> segments) {
        for (CubeSegment seg : segments) {
            boolean op = cube.getSegments().isOperative(seg);
            seg.getAdditionalInfo().put("tmp_op_flag", "" + op);
        }
    }

    public void checkEnableCubeCondition(CubeInstance cube) {
        aclEvaluate.hasProjectWritePermission(cube.getProjectInstance());
        Message msg = MsgPicker.getMsg();
        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();

        Segments<CubeSegment> segments = new Segments<CubeSegment>();

        MPCubeManager mgr = MPCubeManager.getInstance(getConfig());

        List<CubeInstance> mpcubeList = Lists.newArrayList();
        if (mgr.isCommonCube(cube)) {
            segments.addAll(cube.getSegments());
        } else if (mgr.isMPMaster(cube)) {
            mpcubeList = mgr.listMPCubes(cube);
        } else {
            throw new IllegalArgumentException(cube + " must not be a MPCube");
        }

        for (CubeInstance mpcube : mpcubeList) {
            segments.addAll(mpcube.getSegments());
        }

        if (!cube.getStatus().equals(RealizationStatusEnum.DISABLED)) {
            throw new BadRequestException(String.format(msg.getENABLE_NOT_DISABLED_CUBE(), cubeName, ostatus));
        }

        if (segments.getSegments(SegmentStatusEnum.READY).size() == 0) {
            throw new BadRequestException(String.format(msg.getNO_READY_SEGMENT(), cubeName));
        }

        if (!cube.getDescriptor().checkSignature()) {
            throw new BadRequestException(
                    String.format(msg.getINCONSISTENT_CUBE_DESC_SIGNATURE(), cube.getDescriptor()));
        }
    }

    public void validateMPDimensions(CubeDesc cubeDesc, KapModel model) {
        TblColRef[] mpCols = model.getMutiLevelPartitionCols();
        RowKeyColDesc[] rowKeyCols = cubeDesc.getRowkey().getRowKeyColumns();
        for (TblColRef c : mpCols) {
            if (existInRowKeys(c, rowKeyCols) == false) {
                KapMessage msg = KapMsgPicker.getMsg();
                throw new BadRequestException(msg.getMPCUBE_REQUIRES_MPCOLS() + ": " + c.getIdentity());
            }
        }
    }

    private boolean existInRowKeys(TblColRef c, RowKeyColDesc[] rowKeyCols) {
        for (RowKeyColDesc rkey : rowKeyCols) {
            if (rkey.getColumn().equals(c.getIdentity()))
                return true;
        }
        return false;
    }

}
