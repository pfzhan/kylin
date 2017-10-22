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

import org.apache.kylin.cube.CubeSegmentAdvisor;
import org.apache.kylin.metadata.model.ISegment;

/**
 * This class is not used but only holding below document.
 * 
 * Before a full refactoring, here is the hack design.
 * 
 * There are 3 mixed confusing concepts:
 * 
 * - TSRange that is used to build a segment by date range, call the cube DR-Cube.
 * - SegRange that is used to build a segment by general source offset range, call the cube SO-Cube.
 * - TSRange is also used as the data time range of a segment to do segment pruning at query time,
 *   applying to both DR-Cube and SO-Cube.
 * 
 * How they work together now:
 * 
 * - When building a segment, specifying EITHER TSRange OR SegRange. And that defines whether
 *   this is a DR-Cube or SO-Cube. Once defined, all following segments must build in the same way.
 * - Whether a cube is DR-Cube or SO-Cube can be checked by "isOffsetCube()".
 * - After build, for both DR-Cube and SO-Cube, "getSegRange()" provides a view of the segment range,
 *   which tells apart the source ranges of segments.
 * - After build, for both DR-Cube and SO-Cube, "getTSRange()" provides a view of the data time range,
 *   which can be used for segment pruning.
 * - For DR-Cube, "getSegRange()" and "getTSRange()" return the same object.
 * - For SO-Cube, "getSegRange()" and "getTSRange()" return different information.
 * 
 * Future work:
 * 
 * - All cubes should be SO-Cube in the future. We are in the middle of transition.
 */
public class KapCubeSegmentAdvisor extends CubeSegmentAdvisor {

    public KapCubeSegmentAdvisor(ISegment segment) {
        super(segment);
    }

}
