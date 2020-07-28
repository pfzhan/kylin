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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.RecItemV2;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class RawRecItem implements IKeep {
    private static final String TYPE_ERROR_FORMAT = "incorrect raw recommendation type(%d), type value must from 1 to 4 included";
    private static final String STATE_ERROR_FORMAT = "incorrect raw recommendation state(%d), type value must from 0 to 4 included";

    private int id;
    private String project;
    private String modelID;
    private String uniqueFlag;
    private int semanticVersion;
    private RawRecType type;
    private RecItemV2 recEntity;
    private RawRecState state;
    private long createTime;
    private long updateTime;
    private int[] dependIDs;

    // only for raw layout recommendation
    private LayoutMetric layoutMetric;
    private int hitCount;
    private double cost;
    private double totalLatencyOfLastDay;
    private double totalTime;
    private double maxTime;
    private double minTime;
    private String queryHistoryInfo;

    // reserved fields
    private String reserveField1;
    private String reserveField2;
    private String reserveField3;

    public RawRecItem() {
    }

    public RawRecItem(String project, String modelID, int semanticVersion, RawRecType type) {
        this();
        this.project = project;
        this.modelID = modelID;
        this.semanticVersion = semanticVersion;
        this.type = type;
    }

    public boolean needCache() {
        return id != 0 && state == RawRecItem.RawRecState.INITIAL && type != RawRecType.LAYOUT;
    }

    public boolean isOutOfDate(int semanticVersion) {
        return getSemanticVersion() < semanticVersion;
    }

    public boolean isAgg() {
        Preconditions.checkState(RawRecItem.RawRecType.LAYOUT == getType());
        return ((LayoutRecItemV2) getRecEntity()).isAgg();
    }

    /**
     * Raw recommendation type
     */
    public enum RawRecType {
        COMPUTED_COLUMN(1), DIMENSION(2), MEASURE(3), LAYOUT(4);

        private int id;

        public int id() {
            return this.id;
        }

        RawRecType(int id) {
            this.id = id;
        }
    }

    /**
     * Raw recommendation state
     */
    public enum RawRecState {
        INITIAL(0), RECOMMENDED(1), APPLIED(2), DISCARD(3), BROKEN(4);

        private int id;

        public int id() {
            return this.id;
        }

        RawRecState(int id) {
            this.id = id;
        }
    }

    public static int[] toDependIds(String jsonString) {
        try {
            return JsonUtil.readValue(jsonString, int[].class);
        } catch (IOException e) {
            throw new IllegalStateException("cannot deserialize depend id correctly", e);
        }
    }

    public static RawRecItem.RawRecType toRecType(byte recType) {
        switch (recType) {
        case 1:
            return RawRecItem.RawRecType.COMPUTED_COLUMN;
        case 2:
            return RawRecItem.RawRecType.DIMENSION;
        case 3:
            return RawRecItem.RawRecType.MEASURE;
        case 4:
            return RawRecItem.RawRecType.LAYOUT;
        default:
            throw new IllegalStateException(String.format(RawRecItem.TYPE_ERROR_FORMAT, recType));
        }
    }

    public static RawRecItem.RawRecState toRecState(byte stateType) {
        switch (stateType) {
        case 0:
            return RawRecItem.RawRecState.INITIAL;
        case 1:
            return RawRecItem.RawRecState.RECOMMENDED;
        case 2:
            return RawRecItem.RawRecState.APPLIED;
        case 3:
            return RawRecItem.RawRecState.DISCARD;
        case 4:
            return RawRecItem.RawRecState.BROKEN;
        default:
            throw new IllegalStateException(String.format(RawRecItem.STATE_ERROR_FORMAT, stateType));
        }
    }

    public static RecItemV2 toRecItem(String jsonString, byte recType) {
        try {
            switch (recType) {
            case 1:
                return JsonUtil.readValue(jsonString, CCRecItemV2.class);
            case 2:
                return JsonUtil.readValue(jsonString, DimensionRecItemV2.class);
            case 3:
                return JsonUtil.readValue(jsonString, MeasureRecItemV2.class);
            case 4:
                return JsonUtil.readValue(jsonString, LayoutRecItemV2.class);
            default:
                throw new IllegalStateException(String.format(RawRecItem.TYPE_ERROR_FORMAT, recType));
            }
        } catch (IOException | IllegalStateException e) {
            throw new IllegalStateException("cannot deserialize recommendation entity.", e);
        }
    }
}
