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
import java.util.List;
import java.util.WeakHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.CubeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.modeling.smart.cube.CubeOptimizeLogManager;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.response.ColumnarResponse;

@Component("kapCubeService")
public class KapCubeService extends BasicService {
    public static final char[] VALID_CUBENAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();
    private static final Logger logger = LoggerFactory.getLogger(KapCubeService.class);

    protected WeakHashMap<String, ColumnarResponse> columnarInfoCache = new WeakHashMap<>();
    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    public ColumnarResponse getColumnarInfo(String segStoragePath, CubeSegment segment) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String id = segment.getUuid();
        if (columnarInfoCache.containsKey(id)) {
            return columnarInfoCache.get(id);
        }

        RawTableManager rawTableManager = RawTableManager.getInstance(segment.getConfig());
        RawTableInstance raw = rawTableManager.getAccompanyRawTable(segment.getCubeInstance());

        ColumnarResponse columnarResp = new ColumnarResponse();
        columnarResp.setDateRangeStart(segment.getDateRangeStart());
        columnarResp.setDateRangeEnd(segment.getDateRangeEnd());

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
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
            List<RawTableSegment> rawSegs = rawTableManager.getRawtableSegmentByDataRange(raw,
                    segment.getDateRangeStart(), segment.getDateRangeEnd());
            if (rawSegs.size() != 1)
                throw new BadRequestException(msg.getRAW_SEG_SIZE_NOT_ONE());

            String rawSegmentDir = getRawParquetFolderPath(rawSegs.get(0));
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

        columnarInfoCache.put(id, columnarResp);
        return columnarResp;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#oldCube, 'ADMINISTRATION') or hasPermission(#oldCube, 'MANAGEMENT')")
    public CubeInstance cubeClone(CubeInstance oldCube, String newCubeName, String project) throws IOException {
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

        CubeInstance newCube = cubeService.createCubeAndDesc(newCubeName, project, newCubeDesc);

        //reload to avoid shallow clone
        cubeService.getCubeDescManager().reloadCubeDescLocal(newCubeName);

        return newCube;
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

}
