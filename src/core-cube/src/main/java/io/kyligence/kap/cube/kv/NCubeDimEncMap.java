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
package io.kyligence.kap.cube.kv;

import com.google.common.collect.Maps;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NEncodingDesc;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class NCubeDimEncMap implements IDimensionEncodingMap, java.io.Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NCubeDimEncMap.class);

    final private NDataSegment dataSegment;
    final private NCubePlan cubePlan;
    final private Map<TblColRef, Dictionary<String>> dictionaryMap;
    final private Map<TblColRef, DimensionEncoding> encMap = Maps.newHashMap();

    public NCubeDimEncMap(NDataSegment nDataSegment) {
        this.dataSegment = nDataSegment;
        this.cubePlan = nDataSegment.getCubePlan();
        NCubePlan cubePlan = nDataSegment.getCubePlan();
        dictionaryMap = Maps.newHashMap();
        for (TblColRef colRef : cubePlan.getAllColumnsHaveDictionary()) {
            Dictionary<String> dict = nDataSegment.getDictionary(colRef);
            if (dict == null) {
                logger.error("Dictionary for {} was not found.", colRef.getIdentity());
                continue;
            }
            dictionaryMap.put(colRef, dict);
        }
    }

    @Override
    public DimensionEncoding get(TblColRef col) {
        DimensionEncoding result = encMap.get(col);
        if (result == null) {
            NEncodingDesc encodingDesc = cubePlan.getDimensionEncoding(col);
            if (encodingDesc.getName().equals(DictionaryDimEnc.ENCODING_NAME)) {
                // special dictionary encoding
                Dictionary<String> dict = getDictionary(col);
                if (dict == null) {
                    logger.warn("No dictionary found for dict-encoding column " + col + ", segment " + dataSegment);
                    result = new FixedLenDimEnc(0);
                } else {
                    result = new DictionaryDimEnc(dict);
                }
            } else {
                // other case
                result = DimensionEncodingFactory.create(encodingDesc.getEncodingName(), encodingDesc.getEncodingArgs(),
                        encodingDesc.getVersion());
            }
            encMap.put(col, result);
        }
        return result;
    }

    @Override
    public Dictionary<String> getDictionary(TblColRef col) {
        if (dataSegment == null)
            return dictionaryMap.get(col);
        else
            return dataSegment.getDictionary(col);
    }

}
