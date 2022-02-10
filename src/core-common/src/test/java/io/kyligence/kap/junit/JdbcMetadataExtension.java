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
package io.kyligence.kap.junit;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

import java.lang.reflect.Method;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.SneakyThrows;
import lombok.val;

public class JdbcMetadataExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        getTestConfig().setMetadataUrl(getTableName(context)
                + "@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == JdbcInfo.class;
    }

    @SneakyThrows
    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        val result = new JdbcInfo();
        result.setJdbcTemplate(getJdbcTemplate());
        result.setTableName(getTableName(extensionContext));
        return result;
    }

    private String getTableName(ExtensionContext context) {
        return "test_" + context.getTestMethod().map(Method::getName).orElse("") + "_"
                + context.getDisplayName().replaceAll("[|]|\\W", "_");
    }

    private JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

}
