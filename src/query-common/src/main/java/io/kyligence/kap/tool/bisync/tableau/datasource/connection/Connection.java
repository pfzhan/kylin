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
package io.kyligence.kap.tool.bisync.tableau.datasource.connection;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Connection {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String className;

    @JacksonXmlProperty(localName = "dbname", isAttribute = true)
    private String dbName;

    @JacksonXmlProperty(localName = "odbc-connect-string-extras", isAttribute = true)
    private String odbcConnectStringExtras;

    @JacksonXmlProperty(localName = "odbc-dbms-name", isAttribute = true)
    private String odbcDbmsName;

    @JacksonXmlProperty(localName = "odbc-driver", isAttribute = true)
    private String odbcDriver;

    @JacksonXmlProperty(localName = "odbc-dsn", isAttribute = true)
    private String odbcDsn;

    @JacksonXmlProperty(localName = "odbc-suppress-connection-pooling", isAttribute = true)
    private String odbcSuppressConnectionPooling;

    @JacksonXmlProperty(localName = "odbc-use-connection-pooling", isAttribute = true)
    private String odbcUseConnectionPooling;

    @JacksonXmlProperty(localName = "port", isAttribute = true)
    private String port;

    @JacksonXmlProperty(localName = "schema", isAttribute = true)
    private String schema;

    @JacksonXmlProperty(localName = "server", isAttribute = true)
    private String server;

    @JacksonXmlProperty(localName = "username", isAttribute = true)
    private String userName;

    @JacksonXmlProperty(localName = "connection-customization", isAttribute = true)
    private ConnectionCustomization connectionCustomization;

    public void setOdbcConnectStringExtras(String odbcConnectStringExtras) {
        this.odbcConnectStringExtras = odbcConnectStringExtras;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
