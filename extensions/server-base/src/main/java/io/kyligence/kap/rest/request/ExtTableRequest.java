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

package io.kyligence.kap.rest.request;

public class ExtTableRequest {
    public ExtTableRequest() {
    }

    private String[] tables;

    private String project;

    private boolean needProfile = true;

    private float ratio = 1.0f;

    public void setRatio(float ratio) {
        this.ratio = ratio;
    }

    public float getRatio() {
        return this.ratio;
    }

    public int getFrequency() {
        if (Math.abs(ratio) < 1e-6)
            return 1;

        return (int) (1 / ratio);
    }

    public String[] getTables() {
        return tables;
    }

    public String getProject() {
        return project;
    }

    public boolean isNeedProfile() {
        return needProfile;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void setNeedProfile(boolean needProfile) {
        this.needProfile = needProfile;
    }

}
