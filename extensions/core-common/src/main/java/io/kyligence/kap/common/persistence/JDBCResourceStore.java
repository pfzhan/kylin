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

package io.kyligence.kap.common.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class JDBCResourceStore extends ResourceStore {

    private static final String JDBC_SCHEME = "jdbc";

    private String tableName;

    private JDBCResourceDAO resourceDAO;

    public JDBCResourceStore(KylinConfig kylinConfig, StorageURL storageUrl) throws IOException, SQLException {
        super(kylinConfig, storageUrl);
        checkScheme(storageUrl);
        this.tableName = storageUrl.getIdentifier();
        this.resourceDAO = new JDBCResourceDAO(kylinConfig, tableName);
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        try {
            return resourceDAO.existResource(resPath);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        try {
            JDBCResource resource = resourceDAO.getResource(resPath, true, true);
            if (resource != null)
                return new RawResource(resource.getContent(), resource.getTimestamp());
            else
                return null;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        try {
            JDBCResource resource = resourceDAO.getResource(resPath, false, true);
            if (resource != null) {
                return resource.getTimestamp();
            } else {
                return 0L;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        try {
            final TreeSet<String> result = resourceDAO.listAllResource(makeFolderPath(folderPath));
            return result.isEmpty() ? null : result;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException {
        final List<RawResource> result = Lists.newArrayList();
        try {
            List<JDBCResource> allResource = resourceDAO.getAllResource(makeFolderPath(folderPath), timeStart, timeEndExclusive);
            for (JDBCResource resource : allResource) {
                result.add(new RawResource(resource.getContent(), resource.getTimestamp()));
            }
            return result;
        } catch (SQLException e) {
            for (RawResource rawResource : result) {
                IOUtils.closeQuietly(rawResource.inputStream);
            }
            throw new IOException(e);
        }
    }

    @Override
    protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException {
        try {
            JDBCResource resource = new JDBCResource(resPath, ts, content);
            resourceDAO.putResource(resource);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException {
        try {
            resourceDAO.checkAndPutResource(resPath, content, oldTS, newTS);
            return newTS;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        try {
            resourceDAO.deleteResource(resPath);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return tableName + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    private String makeFolderPath(String folderPath) {
        Preconditions.checkState(folderPath.startsWith("/"));
        String lookForPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        return lookForPrefix;
    }

    protected JDBCResourceDAO getResourceDAO() {
        return resourceDAO;
    }
    
    public static void checkScheme(StorageURL url) {
        Preconditions.checkState(JDBC_SCHEME.equals(url.getScheme()));
    }
}