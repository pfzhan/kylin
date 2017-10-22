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
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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

    // exclude MP cube from RealizationChooser
    public static class RealizationFilter implements IRealizationFilter {

        final MPCubeManager mgr;

        public RealizationFilter(KylinConfig config) {
            mgr = getInstance(config);
        }

        @Override
        public boolean accept(IRealization real) {
            if (real instanceof CubeInstance) {
                // accept Common or MPMaster, refuse MPCube
                return !mgr.isMPCube((CubeInstance) real);
            }
            return true;
        }
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
    public List<String> listMPValues(CubeInstance cube) {
        if (isMPMaster(cube) == false) {
            throw new IllegalStateException(
                    "Invalid cube type, current type is not a master cube, name: '" + cube.getName() + "'");
        }

        String mpcubeMatchName = buildMPCubeOwner(cube.getName());

        List<String> result = Lists.newArrayList();
        List<CubeInstance> mpcubeList = listAllMPCubes();
        for (CubeInstance ci : mpcubeList) {
            if (ci.getOwner().equals(mpcubeMatchName)) {
                String mpcubeName = StringUtils.removeStart(ci.getName(), cube.getName() + V_SPLIT);
                result.add(decode(mpcubeName));
            }
        }
        return result;
    }

    // all mpcube must have segments
    private List<CubeInstance> listAllMPCubes() {
        List<CubeInstance> result = Lists.newArrayList();
        List<CubeInstance> cubeInsList = CubeManager.getInstance(config).listAllCubes();
        for (CubeInstance ci : cubeInsList) {
            if (isMPCube(ci) == false)
                continue;

            if (ci.getSegments().size() > 0) {
                result.add(ci);
            }
        }
        return result;
    }

    // FIXME dup with listMPCube(CubeInstance), keep only one of them
    public List<CubeInstance> listAllMPCubes(String masterName) {
        List<CubeInstance> result = Lists.newArrayList();

        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(masterName);
        if (isMPMaster(cubeInstance) == false)
            throw new IllegalStateException(masterName + " is not a MP Master");

        String matchOwner = buildMPCubeOwner(masterName);

        List<CubeInstance> cubeInsList = CubeManager.getInstance(config).listAllCubes();
        for (CubeInstance ci : cubeInsList) {
            if (StringUtils.isBlank(ci.getOwner())) {
                continue;
            }

            if (ci.getOwner().equals(matchOwner) && ci.getSegments().size() > 0) {
                result.add(ci);
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
        // FIXME forget specify UTF-8 encoding?
        return new String(d);
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
                    // FIXME not "0" ??
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
        // FIXME should split by "_" first then decode, the exact reverse of buildMPCubeName()
        String mpValues = decode(mpValuesHex);
        String[] sp = mpValues.split(V_SPLIT);
        String[] result = new String[sp.length];
        for (int i = 0; i < sp.length; i++) {
            result[i] = sp[i];
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

    public List<CubeInstance> listMPCube(CubeInstance master) {
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
