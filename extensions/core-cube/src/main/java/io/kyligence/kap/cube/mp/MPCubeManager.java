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

package io.kyligence.kap.cube.mp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.KapModel;

/**
 * MPMaster ->> Multilevel partition master cube.
 * MPCube ->> Multilevel partition clone cube, cloned from master cube.
 */
public class MPCubeManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(MPCubeManager.class);

    private static String MPMASTER_PRIFIX = "MPMASTER_";

    private static String V_SPLIT = "_";

    public static MPCubeManager getInstance(KylinConfig config) {
        return new MPCubeManager(config);
    }

    // ============================================================================

    private final KylinConfig config;

    private MPCubeManager(KylinConfig conf) {
        this.config = conf;
    }

    //mpMaster ->> mpCube
    public CubeInstance convertToMPCubeIfNeeded(String cubeName, String[] mpValues) throws IOException {
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(cubeName);
        KapModel kapModel = (KapModel) cubeInstance.getModel();

        if (isCommonCube(cubeInstance)) {
            return cubeInstance;
        }

        if (isMPCube(cubeInstance)) {
            return cubeInstance;
        }

        checkMPCubeValues(kapModel, mpValues);

        cubeInstance = checkoutMPCube(cubeInstance, mpValues);

        return cubeInstance;
    }

    //mpCube ->> mpMaster
    public CubeInstance convertToMPMasterIfNeeded(String cubeName) {
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(cubeName);

        if (isCommonCube(cubeInstance)) {
            return cubeInstance;
        }

        if (isMPMaster(cubeInstance)) {
            return cubeInstance;
        }

        String mpMasterName = StringUtils.removeStart(cubeInstance.getOwner(), MPMASTER_PRIFIX);
        cubeInstance = CubeManager.getInstance(config).getCube(mpMasterName);

        return cubeInstance;
    }

    // drop MPCube, if it has no segments
    public void dropMPCubeIfNeeded(String cubeName) throws IOException {
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(cubeName);

        if (isCommonCube(cubeInstance)) {
            return;
        }

        if (isMPMaster(cubeInstance)) {
            return;
        }

        Segments<CubeSegment> segments = cubeInstance.getSegments();
        if (segments.isEmpty()) {
            CubeManager.getInstance(config).dropCube(cubeName, false);
        }
    }

    // List all mpcube mpvalues of a mpmaster
    public List<Map<String, Object>> listMPValuesAndCubes(CubeInstance cube) {
        if (isMPMaster(cube) == false) {
            throw new IllegalStateException(
                    "Invalid cube type, current type is not a master cube, name: '" + cube.getName() + "'");
        }

        String mpcubeMatchName = buildMPCubeOwner(cube.getName());

        List<Map<String, Object>> result = Lists.newArrayList();
        List<CubeInstance> mpcubeList = listMPCubes(cube);
        for (CubeInstance ci : mpcubeList) {
            if (ci.getOwner().equals(mpcubeMatchName)) {
                Map<String, Object> map = Maps.newHashMap();
                String mpcubeName = StringUtils.removeStart(ci.getName(), cube.getName() + V_SPLIT);
                map.put("name", decode(mpcubeName));
                map.put("cube", ci);
                result.add(map);
            }
        }
        return result;
    }

    String decode(String cubeNameHex) {
        if (StringUtils.isBlank(cubeNameHex)) {
            return null;
        }
        cubeNameHex = cubeNameHex.toUpperCase();
        int length = cubeNameHex.length() / 2;
        char[] hexChars = cubeNameHex.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }

        String result = null;
        try {
            result = new String(d, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Hex string decode error. Hex: '" + cubeNameHex + "'", e);
        }

        return result;
    }

    //Convert char to byte
    private byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    //Convert param string to hex.
    String encode(String param) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            byte[] ba = param.getBytes("UTF-8");
            for (int i = 0; i < ba.length; i++) {
                int v = ba[i] & 0xFF;
                String hv = Integer.toHexString(v);
                if (hv.length() < 2) {
                    stringBuilder.append(0);
                }
                stringBuilder.append(hv);
            }
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("param getBytes error. param: '" + param + "' ", e);
        }
        return stringBuilder.toString();
    }

    public String[] fetchMPValues(CubeInstance cube) {
        String cubeName = cube.getName();
        String mpMasterName = StringUtils.removeStart(cube.getOwner(), MPMASTER_PRIFIX);
        String mpValuesHex = StringUtils.removeStart(cubeName, mpMasterName + V_SPLIT);
        String[] mpHex = mpValuesHex.split(V_SPLIT);
        String[] result = new String[mpHex.length];
        for (int i = 0; i < mpHex.length; i++) {
            result[i] = decode(mpHex[i]);
        }
        return result;
    }

    public boolean isCommonCube(CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();
        KapModel kapModel = (KapModel) cubeDesc.getModel();

        if (kapModel.isMultiLevelPartitioned()) {
            return false;
        }

        return true;
    }

    // checks if a cube is a MP master
    public boolean isMPMaster(CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();
        KapModel kapModel = (KapModel) cubeDesc.getModel();

        if (!kapModel.isMultiLevelPartitioned()) {
            return false;
        }

        if (cube.getOwner() != null && cube.getOwner().startsWith(MPMASTER_PRIFIX)) {
            return false;
        }

        return true;
    }

    // checks if a cube is a MP cube (excluding MP master)
    public boolean isMPCube(CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();
        KapModel kapModel = (KapModel) cubeDesc.getModel();

        if (!kapModel.isMultiLevelPartitioned()) {
            return false;
        }

        if (cube.getOwner() != null && cube.getOwner().startsWith(MPMASTER_PRIFIX)) {
            return true;
        }

        return false;
    }

    public void checkMPCubeValues(KapModel model, String[] mpValues) {
        TblColRef[] tblColRefs = model.getMutiLevelPartitionCols();
        if (tblColRefs.length != mpValues.length) {
            throw new IllegalStateException("Invalid cube column length : '" + tblColRefs.length
                    + "' does match param length : '" + mpValues.length + "'");
        }

        checkMPColumnParams(tblColRefs, mpValues);
    }

    private CubeInstance checkoutMPCube(CubeInstance mpMaster, String[] mpValues) throws IOException {
        String mpCubeName = buildMPCubeName(mpMaster.getName(), mpValues);
        CubeInstance mpCube = CubeManager.getInstance(config).getCube(mpCubeName);
        if (null == mpCube) {
            mpCube = createMPCube(mpMaster, mpValues);
        }

        return mpCube;
    }

    CubeInstance createMPCube(CubeInstance mpMaster, String[] mpValues) throws IOException {
        String name = buildMPCubeName(mpMaster.getName(), mpValues);
        String owner = buildMPCubeOwner(mpMaster.getName());

        CubeDesc mpMasterDesc = mpMaster.getDescriptor();

        CubeInstance mpCube = CubeManager.getInstance(config).createCube(name, mpMaster.getProject(), mpMasterDesc,
                owner);

        return mpCube;
    }

    String buildMPCubeName(String name, String[] mpValues) {
        List partList = Lists.newArrayList();
        partList.add(name);
        for (String pvalue : mpValues) {
            partList.add(encode(pvalue));
        }

        return StringUtil.join(partList, V_SPLIT);
    }

    String buildMPCubeOwner(String name) {
        return MPMASTER_PRIFIX + name;
    }

    public List<CubeInstance> listMPCubes(CubeInstance master) {
        if (!isMPMaster(master))
            throw new IllegalArgumentException();

        String ownerMatch = buildMPCubeOwner(master.getName());

        List<CubeInstance> list = CubeManager.getInstance(config).listAllCubes();
        List<CubeInstance> result = new ArrayList<CubeInstance>();

        for (CubeInstance c : list) {
            if (ownerMatch.equals(c.getOwner())) {
                result.add(c);
            }
        }

        return result;
    }

    // @TODO Need check other datatype.
    private void checkMPColumnParams(TblColRef[] tblColRefs, String[] paramValues) {
        for (int i = 0; i < tblColRefs.length; i++) {
            DataType dataType = tblColRefs[i].getType();
            if (StringUtils.isBlank(paramValues[i])) {
                throw new IllegalArgumentException("Invalid param value, paramValue is null.");
            }

            //current status. just check param value is NumberFamily.
            if (dataType.isNumberFamily()) {
                if (!StringUtils.isNumeric(paramValues[i])) {
                    throw new IllegalArgumentException("Invalid column datatype value: datatype: '"
                            + tblColRefs[i].getDatatype() + "' value:" + paramValues[i]);
                }
            }
        }
    }
}
