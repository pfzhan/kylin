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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class JDBCResourceDAO {

    private static Logger logger = LoggerFactory.getLogger(JDBCResourceDAO.class);

    private static final String META_TABLE_KEY = "META_TABLE_KEY";

    private static final String META_TABLE_TS = "META_TABLE_TS";

    private static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";

    private JDBCConnectionManager connectionManager;

    private JDBCSqlQueryFormat jdbcSqlQueryFormat;

    private String tableName;

    private KapConfig kapConfig;

    private KylinConfig kylinConfig;

    public JDBCResourceDAO(KylinConfig kylinConfig, String tableName) throws IOException, SQLException {
        this.tableName = tableName;
        this.kylinConfig = kylinConfig;
        this.kapConfig = KapConfig.wrap(kylinConfig);
        this.connectionManager = JDBCConnectionManager.getConnectionManager();
        this.jdbcSqlQueryFormat = JDBCSqlQueryFormatProvider.createJDBCSqlQueriesFormat(kapConfig.getMetadataDialect());
        createTableIfNeeded(tableName);
    }

    public JDBCResource getResource(final String resourcePath, final boolean fetchContent, final boolean fetchTimestamp) throws SQLException {
        final JDBCResource resource = new JDBCResource();
        logger.trace("getResource method. resourcePath : {} , fetchConetent : {} , fetch TS : {}", resourcePath, fetchContent, fetchTimestamp);
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getKeyEqualSqlString(fetchContent, fetchTimestamp));
                pstat.setString(1, resourcePath);
                rs = pstat.executeQuery();
                if (rs.next()) {
                    resource.setPath(rs.getString(META_TABLE_KEY));
                    if (fetchTimestamp)
                        resource.setTimestamp(rs.getLong(META_TABLE_TS));
                    if (fetchContent) {
                        try {
                            resource.setContent(getInputStream(resourcePath, rs));
                        } catch (IOException e) {
                            throw new SQLException(e);
                        }
                    }
                }
            }
        });
        if (resource.getPath() != null) {
            return resource;
        } else {
            return null;
        }
    }

    public boolean existResource(final String resourcePath) throws SQLException {
        JDBCResource resource = getResource(resourcePath, false, false);
        return (resource != null);
    }

    public long getResourceTimestamp(final String resourcePath) throws SQLException {
        JDBCResource resource = getResource(resourcePath, false, true);
        return resource == null ? 0 : resource.getTimestamp();
    }

    //fetch primary key only
    public TreeSet<String> listAllResource(final String folderPath) throws SQLException {
        final TreeSet<String> allResourceName = new TreeSet<>();
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getListResourceSqlString());
                pstat.setString(1, folderPath + "%");
                rs = pstat.executeQuery();
                while (rs.next()) {
                    String path = rs.getString(META_TABLE_KEY);
                    assert path.startsWith(folderPath);
                    int cut = path.indexOf('/', folderPath.length());
                    String child = cut < 0 ? path : path.substring(0, cut);
                    allResourceName.add(child);
                }
            }
        });
        return allResourceName;
    }

    public List<JDBCResource> getAllResource(final String folderPath, final long timeStart, final long timeEndExclusive) throws SQLException {
        final List<JDBCResource> allResource = Lists.newArrayList();
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getAllResourceSqlString());
                pstat.setString(1, folderPath + "%");
                pstat.setLong(2, timeStart);
                pstat.setLong(3, timeEndExclusive);
                rs = pstat.executeQuery();
                while (rs.next()) {
                    String resPath = rs.getString(META_TABLE_KEY);
                    if (checkPath(folderPath, resPath)) {
                        JDBCResource resource = new JDBCResource();
                        resource.setPath(resPath);
                        resource.setTimestamp(rs.getLong(META_TABLE_TS));
                        try {
                            resource.setContent(getInputStream(resPath, rs));
                        } catch (IOException e) {
                            throw new SQLException(e);
                        }
                        allResource.add(resource);
                    }
                }
            }
        });
        return allResource;
    }

    private boolean checkPath(String lookForPrefix, String resPath) {
        lookForPrefix = lookForPrefix.endsWith("/") ? lookForPrefix : lookForPrefix + "/";
        assert resPath.startsWith(lookForPrefix);
        int cut = resPath.indexOf('/', lookForPrefix.length());
        return (cut < 0);
    }

    public void deleteResource(final String resourcePath) throws SQLException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getDeletePstatSql());
                pstat.setString(1, resourcePath);
                pstat.executeUpdate();
                deleteHDFSResourceIfExist(resourcePath);
            }
        });

    }

    private void deleteHDFSResourceIfExist(String resourcePath) throws SQLException {
        try {
            Path redirectPath = bigCellHDFSPath(resourcePath);
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

            if (fileSystem.exists(redirectPath)) {
                fileSystem.delete(redirectPath, true);
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void putResource(final JDBCResource resource) throws SQLException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                byte[] content = getResourceDataBytes(resource);
                if (isContentOverflow(content)) {
                    //System.out.println("overflow put resource to hdfs");
                    if (existResource(resource.getPath())) {
                        pstat = connection.prepareStatement(getReplaceSqlWithoutContent());
                        pstat.setLong(1, resource.getTimestamp());
                        pstat.setString(2, resource.getPath());
                    } else {
                        pstat = connection.prepareStatement(getInsertSqlWithoutContent());
                        pstat.setString(1, resource.getPath());
                        pstat.setLong(2, resource.getTimestamp());
                    }
                    pstat.executeUpdate();
                    writeLargeCellToHdfs(resource.getPath(), content);
                } else {
                    //System.out.println("not overflow.put to mysql");
                    if (existResource(resource.getPath())) {
                        pstat = connection.prepareStatement(getReplaceSql());
                        pstat.setLong(1, resource.getTimestamp());
                        pstat.setBlob(2, new BufferedInputStream(new ByteArrayInputStream(content)));
                        pstat.setString(3, resource.getPath());
                    } else {
                        pstat = connection.prepareStatement(getInsertSql());
                        pstat.setString(1, resource.getPath());
                        pstat.setLong(2, resource.getTimestamp());
                        pstat.setBlob(3, new BufferedInputStream(new ByteArrayInputStream(content)));
                    }

                    pstat.executeUpdate();
                }
            }
        });
    }

    public void checkAndPutResource(final String resPath, final byte[] content, final long oldTS, final long newTS) throws SQLException {
        logger.trace("execute checkAndPutResource method. resPath : {} , oldTs : {} , newTs : {} , content null ? : {} ", resPath, oldTS, newTS, content == null);
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                if (!existResource(resPath)) {
                    if (oldTS != 0) {
                        throw new IllegalStateException("For not exist file. OldTS have to be 0. but Actual oldTS is : " + oldTS);
                    }
                    if (isContentOverflow(content)) {
                        pstat = connection.prepareStatement(getInsertSqlWithoutContent());
                        pstat.setString(1, resPath);
                        pstat.setLong(2, newTS);
                        pstat.executeUpdate();
                        writeLargeCellToHdfs(resPath, content);
                    } else {
                        pstat = connection.prepareStatement(getInsertSql());
                        pstat.setString(1, resPath);
                        pstat.setLong(2, newTS);
                        pstat.setBlob(3, new BufferedInputStream(new ByteArrayInputStream(content)));
                        pstat.executeUpdate();
                    }
                } else {
                    pstat = connection.prepareStatement(getUpdateSqlWithoutContent());
                    pstat.setLong(1, newTS);
                    pstat.setString(2, resPath);
                    pstat.setLong(3, oldTS);
                    int result = pstat.executeUpdate();
                    if (result != 1) {
                        long realTime = getResourceTimestamp(resPath);
                        throw new IllegalStateException("Overwriting conflict " + resPath + ", expect old TS " + oldTS + ", but it is " + realTime);
                    }
                    if (isContentOverflow(content)) {
                        writeLargeCellToHdfs(resPath, content);
                    } else {
                        PreparedStatement pstat2 = null;
                        try {
                            //"update {0} set {1}=? where {3}=? "
                            pstat2 = connection.prepareStatement(getUpdateContentSql());
                            pstat2.setBinaryStream(1, new BufferedInputStream(new ByteArrayInputStream(content)));
                            pstat2.setString(2, resPath);
                            pstat2.executeUpdate();
                        } finally {
                            JDBCConnectionManager.closeQuietly(pstat2);
                        }
                    }
                }
            }
        });
    }

    private byte[] getResourceDataBytes(JDBCResource resource) throws SQLException {
        ByteArrayOutputStream bout = null;
        try {
            bout = new ByteArrayOutputStream();
            IOUtils.copy(resource.getContent(), bout);
            byte[] content = bout.toByteArray();
            return content;
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            IOUtils.closeQuietly(bout);
        }
    }

    private boolean isContentOverflow(byte[] content) throws SQLException {
        int maxSize = kapConfig.getJdbcResourceStoreMaxCellSize();
        if (content.length > maxSize)
            return true;
        else
            return false;
    }

    private void createTableIfNeeded(final String tableName) throws IOException, SQLException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getCreateIfNeededSql(tableName));
                pstat.executeUpdate();
            }
        });
    }

    abstract static class SqlOperation {
        PreparedStatement pstat = null;
        ResultSet rs = null;

        abstract public void execute(final Connection connection) throws SQLException;
    }

    private void executeSql(SqlOperation operation) throws SQLException {
        Connection connection = null;
        try {
            connection = connectionManager.getConn();
            operation.execute(connection);
        } finally {
            JDBCConnectionManager.closeQuietly(operation.rs);
            JDBCConnectionManager.closeQuietly(operation.pstat);
            JDBCConnectionManager.closeQuietly(connection);
        }
    }

    //sql queries
    private String getCreateIfNeededSql(String tableName) {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getCreateIfNeedSql(),
                tableName, META_TABLE_KEY, META_TABLE_TS, META_TABLE_CONTENT);
        return sql;
    }

    private String getKeyEqualSqlString(boolean fetchContent, boolean fetchTimestamp) {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getKeyEqualsSql(), getSelectList(fetchContent, fetchTimestamp), tableName,
                META_TABLE_KEY);
        return sql;
    }

    private String getDeletePstatSql() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getDeletePstatSql(), tableName, META_TABLE_KEY);
        return sql;
    }

    private String getListResourceSqlString() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getListResourceSql(), META_TABLE_KEY, tableName, META_TABLE_KEY);
        return sql;
    }

    private String getAllResourceSqlString() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getAllResourceSql(), getSelectList(true, true), tableName, META_TABLE_KEY,
                META_TABLE_TS, META_TABLE_TS);
        return sql;
    }

    private String getReplaceSql() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getReplaceSql(), tableName, META_TABLE_TS, META_TABLE_CONTENT, META_TABLE_KEY);
        return sql;
    }

    private String getInsertSql() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getInsertSql(), tableName, META_TABLE_KEY, META_TABLE_TS, META_TABLE_CONTENT);
        return sql;
    }

    private String getReplaceSqlWithoutContent() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getReplaceSqlWithoutContent(), tableName, META_TABLE_TS, META_TABLE_KEY);
        return sql;
    }

    private String getInsertSqlWithoutContent() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getInsertSqlWithoutContent(), tableName, META_TABLE_KEY, META_TABLE_TS);
        return sql;
    }

    private String getUpdateSqlWithoutContent() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getUpdateSqlWithoutContent(), tableName, META_TABLE_TS, META_TABLE_KEY, META_TABLE_TS);
        return sql;
    }

    private String getUpdateContentSql() {
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getUpdateContentSql(), tableName, META_TABLE_CONTENT, META_TABLE_KEY);
        return sql;
    }

    private String getSelectList(boolean fetchContent, boolean fetchTimestamp) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_TABLE_KEY);
        if (fetchTimestamp)
            sb.append("," + META_TABLE_TS);
        if (fetchContent)
            sb.append("," + META_TABLE_CONTENT);
        return sb.toString();
    }

    private InputStream getInputStream(String resPath, ResultSet rs) throws SQLException, IOException {
        if (rs == null) {
            return null;
        }
        InputStream inputStream =rs.getBlob(META_TABLE_CONTENT) == null ? null : rs.getBlob(META_TABLE_CONTENT).getBinaryStream();
        if (inputStream != null) {
            return inputStream;
        } else {
            Path redirectPath = bigCellHDFSPath(resPath);
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            return fileSystem.open(redirectPath);
        }
    }

    private Path writeLargeCellToHdfs(String resPath, byte[] largeColumn) throws SQLException {
        FSDataOutputStream out = null;
        try {
            Path redirectPath = bigCellHDFSPath(resPath);
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

            if (fileSystem.exists(redirectPath)) {
                fileSystem.delete(redirectPath, true);
            }
            out = fileSystem.create(redirectPath);
            out.write(largeColumn);
            return redirectPath;
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    public Path bigCellHDFSPath(String resPath) {
        String hdfsWorkingDirectory = this.kapConfig.getWriteHdfsWorkingDirectory();
        Path redirectPath = new Path(hdfsWorkingDirectory, "resources-jdbc" + resPath);
        return redirectPath;
    }

}