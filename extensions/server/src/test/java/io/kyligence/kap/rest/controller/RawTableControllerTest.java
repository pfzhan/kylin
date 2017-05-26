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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.HashMap;

import org.apache.kylin.rest.service.JobService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.rest.service.RawTableService;
import io.kyligence.kap.rest.service.ServiceTestBase;

/**
 * Created by wangcheng on 9/27/16.
 */
public class RawTableControllerTest extends ServiceTestBase {

    private RawTableController rawController;
    private RawTableDescController descController;

    @Autowired
    @Qualifier("rawTableService")
    RawTableService rawService;
    @Autowired
    @Qualifier("jobService")
    JobService jobService;

    @Before
    public void setup() throws Exception {
        super.setup();

        rawController = new RawTableController();
        rawController.setRawTableService(rawService);
        rawController.setJobService(jobService);

        descController = new RawTableDescController();
        descController.setRawTableService(rawService);

    }

    @Test
    public void testBasics() throws IOException {
        HashMap<String, RawTableDesc> data = (HashMap<String, RawTableDesc>) descController.getDesc(null,
                "ci_left_join_cube").data;
        RawTableDesc desc = data.get("rawTable");
        Assert.assertNotNull(desc);
        RawTableDesc newDesc = new RawTableDesc();
        String newRawName = desc.getName() + "_test_save";

        newDesc.setName(newRawName);
        newDesc.setModelName(desc.getModelName());
    }
}
