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

package io.kyligence.kap.rest.controller2;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.service.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * CubeController is defined as Restful API entrance for UI.
 *
 * @author jianliu
 */
@Controller
@RequestMapping(value = "/cache")
public class CacheControllerV2 extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CacheControllerV2.class);

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    /**
     * Announce wipe cache to all cluster nodes
     */

    @RequestMapping(value = "/announce/{entity}/{cacheKey}/{event}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void announceWipeCacheV2(@PathVariable String entity, @PathVariable String event,
            @PathVariable String cacheKey) throws IOException {

        cacheService.annouceWipeCache(entity, event, cacheKey);
    }

    /**
     * Wipe cache on this node
     */

    @RequestMapping(value = "/{entity}/{cacheKey}/{event}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void wipeCacheV2(@PathVariable String entity, @PathVariable String event, @PathVariable String cacheKey)
            throws IOException {

        cacheService.notifyMetadataChange(entity, Broadcaster.Event.getEvent(event), cacheKey);
    }

    @RequestMapping(value = "/announce/config", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void hotLoadKylinConfigV2() throws IOException {

        KylinConfig.getInstanceFromEnv().reloadFromSiteProperties();
        cacheService.notifyMetadataChange(Broadcaster.SYNC_ALL, Broadcaster.Event.UPDATE, Broadcaster.SYNC_ALL);
    }

    public void setCacheService(CacheService cacheService) {
        this.cacheService = cacheService;
    }
}
