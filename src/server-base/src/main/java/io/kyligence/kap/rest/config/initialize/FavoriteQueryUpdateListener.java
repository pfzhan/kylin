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
package io.kyligence.kap.rest.config.initialize;

import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class FavoriteQueryUpdateListener implements EventListenerRegistry.ResourceEventListener {
    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        val term = rawResource.getResPath().split("\\/");
        if (!isFavoriteQueryPath(term))
            return;

        // deserialize
        FavoriteQuery favoriteQuery = deserialize(rawResource);
        if (favoriteQuery == null)
            return;

        // update favorite query map
        val project = term[1];
        FavoriteQueryManager.getInstance(config, project).updateFavoriteQueryMap(favoriteQuery);
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        val term = resPath.split("\\/");
        if (!isFavoriteQueryPath(term))
            return;

        val project = term[1];
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(config, project);
        favoriteQueryManager.clearFavoriteQueryMap();
    }

    private boolean isFavoriteQueryPath(String[] term) {
        if (term.length < 3 || !term[2].equals(ResourceStore.FAVORITE_QUERY_RESOURCE_ROOT.substring(1))) {
            return false;
        }

        return true;
    }

    private FavoriteQuery deserialize(RawResource rawResource) {
        FavoriteQuery favoriteQuery = null;
        try (InputStream is = rawResource.getByteSource().openStream();
             DataInputStream din = new DataInputStream(is)) {
            favoriteQuery = new JsonSerializer<>(FavoriteQuery.class).deserialize(din);
        } catch (IOException e) {
            log.warn("error when deserializing resource: {}", rawResource.getResPath());
        }

        return favoriteQuery;
    }
}
