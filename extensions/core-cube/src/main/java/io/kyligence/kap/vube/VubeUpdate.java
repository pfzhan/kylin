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

package io.kyligence.kap.vube;

import io.kyligence.kap.cube.raw.RawTableInstance;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import java.util.List;

public class VubeUpdate {
    private VubeInstance vubeInstance;
    private CubeInstance cubeToAdd = null;
    private RawTableInstance rawTableToAdd = null;
    private List<String> sampleSqls = null;
    private CubeInstance[] cubesToUpdate = null;
    private RealizationStatusEnum status;
    private String owner;
    private String project;
    private int cost = -1;
    private String version;

    public VubeUpdate(VubeInstance vubeInstance) {
        this.vubeInstance = vubeInstance;
    }

    public VubeInstance getVubeInstance() {
        return vubeInstance;
    }

    public VubeUpdate setVubeInstance(VubeInstance vubeInstance) {
        this.vubeInstance = vubeInstance;
        return this;
    }

    public CubeInstance getCubeToAdd() {
        return cubeToAdd;
    }

    public VubeUpdate setCubeToAdd(CubeInstance cubeToAdd) {
        this.cubeToAdd = cubeToAdd;
        return this;
    }

    public RawTableInstance getRawTableToAdd() {
        return rawTableToAdd;
    }

    public VubeUpdate setRawTableToAdd(RawTableInstance rawTableInstance) {
        this.rawTableToAdd = rawTableInstance;
        return this;
    }

    public List<String> getSampleSqls() {
        return sampleSqls;
    }

    public void setSampleSqls(List<String> sampleSqls) {
        this.sampleSqls = sampleSqls;
    }

    public CubeInstance[] getCubesToUpdate() {
        return cubesToUpdate;
    }

    public VubeUpdate setCubesToUpdate(CubeInstance... cubesToUpdate) {
        this.cubesToUpdate = cubesToUpdate;
        return this;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public VubeUpdate setStatus(RealizationStatusEnum status) {
        this.status = status;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public VubeUpdate setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public int getCost() {
        return cost;
    }

    public VubeUpdate setCost(int cost) {
        this.cost = cost;
        return this;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}