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

package io.kyligence.kap.rest.docgen;

import java.io.File;
import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.swagger.models.Swagger;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.Documentation;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.DocumentationCache;
import springfox.documentation.spring.web.json.Json;
import springfox.documentation.spring.web.json.JsonSerializer;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;
import springfox.documentation.swagger2.mappers.ServiceModelToSwagger2Mapper;

@Configuration
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class, HibernateJpaAutoConfiguration.class })
@ImportResource({ "applicationContext.xml", "kylinSecurity.xml" })
@EnableSwagger2
public class RestDocGen {

    // http://localhost:8080/v2/api-docs
    // http://localhost:8080/swagger-ui.html#/
    public static void main(String[] args) throws IOException {

        LocalFileMetadataTestCase.staticCreateTestMetadata();
        System.setProperty("KYLIN_HOME", new File("../../").getCanonicalPath());
        System.setProperty("spring.profiles.active", "testing");

        ApplicationContext ctx = SpringApplication.run(RestDocGen.class, args);
        printSwaggerSpec(ctx);
    }

    private static void printSwaggerSpec(ApplicationContext ctx) {
        DocumentationCache docCache = ctx.getBean(DocumentationCache.class);
        ServiceModelToSwagger2Mapper mapper = ctx.getBean(ServiceModelToSwagger2Mapper.class);
        JsonSerializer jsonSerializer = ctx.getBean(JsonSerializer.class);

        Documentation documentation = docCache.documentationByGroup(Docket.DEFAULT_GROUP_NAME);
        Swagger swagger = mapper.mapDocumentation(documentation);
        swagger.host("localhost:7070");
        swagger.basePath("/kylin");
        Json json = jsonSerializer.toJson(swagger);
        System.out.println(json.value());
    }

    @Bean
    public Docket kapRestDocket() {
        return new Docket(DocumentationType.SWAGGER_2) //
                .select() //
                .paths(PathSelectors.ant("/user/**")) //
                .build();
    }
}
