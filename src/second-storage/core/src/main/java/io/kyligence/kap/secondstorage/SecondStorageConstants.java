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

package io.kyligence.kap.secondstorage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SecondStorageConstants {
    public static final String P_OLD_SEGMENT_IDS = "oldSegmentIds";
    public static final String P_MERGED_SEGMENT_ID = "mergedSegmentId";
    public static final String NODE_REPLICA = "kylin.second-storage.node-replica";

    // config
    public static final String CONFIG_SECOND_STORAGE_CLUSTER = "kylin.second-storage.cluster-config";

    // job
    public static final String STEP_EXPORT_TO_SECOND_STORAGE = "STEP_EXPORT_TO_SECOND_STORAGE";
    public static final String STEP_REFRESH_SECOND_STORAGE = "STEP_REFRESH_SECOND_STORAGE";
    public static final String STEP_MERGE_SECOND_STORAGE = "STEP_MERGE_SECOND_STORAGE";

    public static final String STEP_SECOND_STORAGE_NODE_CLEAN = "STEP_SECOND_STORAGE_NODE_CLEAN";
    public static final String STEP_SECOND_STORAGE_MODEL_CLEAN = "STEP_SECOND_STORAGE_MODEL_CLEAN";
    public static final String STEP_SECOND_STORAGE_SEGMENT_CLEAN = "STEP_SECOND_STORAGE_SEGMENT_CLEAN";
    public static final String STEP_SECOND_STORAGE_INDEX_CLEAN = "STEP_SECOND_STORAGE_INDEX_CLEAN";

    public static final Set<String> SKIP_STEP_RUNNING = new HashSet<>(Arrays.asList(STEP_EXPORT_TO_SECOND_STORAGE,
            STEP_REFRESH_SECOND_STORAGE, STEP_MERGE_SECOND_STORAGE, STEP_SECOND_STORAGE_NODE_CLEAN,
            STEP_SECOND_STORAGE_MODEL_CLEAN, STEP_SECOND_STORAGE_INDEX_CLEAN));
    public static final Set<String> SKIP_JOB_RUNNING = new HashSet<>(Arrays.asList(STEP_SECOND_STORAGE_SEGMENT_CLEAN));

    // internal config
    public static final String PROJECT_MODEL_SEGMENT_PARAM = "projectModelSegmentParam";
    public static final String PROJECT = "project";



    private SecondStorageConstants() {
    }
}
