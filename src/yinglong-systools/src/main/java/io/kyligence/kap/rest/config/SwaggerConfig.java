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
package io.kyligence.kap.rest.config;

import static springfox.documentation.builders.PathSelectors.ant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@Configuration
@ConditionalOnProperty(name = "kylin.swagger.enabled", havingValue = "true")
public class SwaggerConfig {

    public static final String LICENSE = "Apache 2.0";
    public static final String SWAGGER_LICENSE_URL = "http://www.apache.org/licenses/LICENSE-2.0.html";
    public static final String TITLE = "Kyligence Enterprise API";

    @Order(2)
    @Bean(value = "v4 public")
    public Docket restApiOpen() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfoOpen()).groupName("v4 public").select()
                .apis(KylinRequestHandlerSelectors.baseCurrentPackage("io.kyligence.kap.rest.controller.open")).build();
    }

    @Order(1)
    @Bean(value = "ke4 internal")
    public Docket restApiV4() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfoV4()).groupName("v4 internal").select()
                .apis(KylinRequestHandlerSelectors.baseCurrentPackage("io.kyligence.kap.rest.controller")).build();
    }

    @Order(3)
    @Bean(value = "v2 public")
    public Docket restApiV2() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfoV2()).groupName("v2 public").select()
                .paths(ant("/api").negate())
                .apis(KylinRequestHandlerSelectors.baseCurrentPackage("io.kyligence.kap.rest.controller.v2")).build();
    }

    private ApiInfo apiInfoOpen() {
        return new ApiInfoBuilder().title(TITLE).description("Newten Open API").license(LICENSE)
                .licenseUrl(SWAGGER_LICENSE_URL).version("4.0.8").build();
    }

    private ApiInfo apiInfoV4() {
        return new ApiInfoBuilder().title(TITLE).description("Newten API").license(LICENSE)
                .licenseUrl(SWAGGER_LICENSE_URL).version("4.0.7").build();
    }

    private ApiInfo apiInfoV2() {
        return new ApiInfoBuilder().title(TITLE).description("V2 API").license(LICENSE).licenseUrl(SWAGGER_LICENSE_URL)
                .version("3.0.0").build();
    }
}