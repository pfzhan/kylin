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
package io.kyligence.kap.newten.clickhouse;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 *  Duplicated implementation of  {@link ClickHouseContainer} which uses {@link com.github.housepower.jdbc.ClickHouseDriver}
 *  <p/>
 *  We use native driver {@link com.github.housepower.jdbc.ClickHouseDriver} instead.
 */
public class ClickHouseContainerWithNativeJDBC extends JdbcDatabaseContainer<ClickHouseContainerWithNativeJDBC> {
    public static final String NAME = "clickhouse";

    public static final String DRIVER_CLASS_NAME = "com.github.housepower.jdbc.ClickHouseDriver";
    public static final String JDBC_URL_PREFIX = "jdbc:" + NAME + "://";
    public static final String TEST_QUERY = "SELECT version()";


    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("yandex/clickhouse-server");

    public static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

    public static final String DEFAULT_TAG = "20.10.3.30";

    public static final Integer HTTP_PORT = 8123;
    public static final Integer NATIVE_PORT = 9000;

    private String databaseName = "default";
    private String username = "default";
    private String password = "";


    public ClickHouseContainerWithNativeJDBC() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public ClickHouseContainerWithNativeJDBC(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public ClickHouseContainerWithNativeJDBC(final DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        withExposedPorts(HTTP_PORT, NATIVE_PORT);
        waitingFor(
                new HttpWaitStrategy()
                        .forStatusCode(200)
                        .forResponsePredicate("Ok."::equals)
                        .withStartupTimeout(Duration.ofMinutes(1))
        );
    }


    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(NATIVE_PORT);
    }

    @Override
    public String getDriverClassName() {
        return DRIVER_CLASS_NAME;
    }

    @Override
    public String getJdbcUrl() {
        return JDBC_URL_PREFIX + getHost() + ":" + getMappedPort(NATIVE_PORT);
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return TEST_QUERY;
    }

    @Override
    public ClickHouseContainerWithNativeJDBC withUrlParam(String paramName, String paramValue) {
        throw new UnsupportedOperationException("The ClickHouse does not support this");
    }
}
