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



package io.kyligence.kap.metadata.resourcegroup;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class ResourceGroupManagerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetResourceGroup() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        ResourceGroup resourceGroup = manager.getResourceGroup();
        Assert.assertFalse(resourceGroup.isResourceGroupEnabled());
        Assert.assertEquals("/_global/resource_group/relation.json", resourceGroup.getResourcePath());
        Assert.assertTrue(resourceGroup.getResourceGroupEntities().isEmpty());
        Assert.assertTrue(resourceGroup.getResourceGroupMappingInfoList().isEmpty());
        Assert.assertTrue(resourceGroup.getKylinInstances().isEmpty());
    }

    @Test
    public void testUpdateResourceGrop() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
        });
        Assert.assertTrue(manager.getResourceGroup().isResourceGroupEnabled());
    }

    @Test
    public void testResourceGroupInitialized() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        Assert.assertTrue(manager.resourceGroupInitialized());
    }

    @Test
    public void testIsResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        Assert.assertFalse(manager.isResourceGroupEnabled());
    }

    @Test
    public void testIsProjectBindToResourceGroup() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        Assert.assertTrue(manager.isProjectBindToResourceGroup("_global"));
        Assert.assertFalse(manager.isProjectBindToResourceGroup("default"));
    }

    @Test
    public void testInitResourceGroup() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.initResourceGroup();
    }
}