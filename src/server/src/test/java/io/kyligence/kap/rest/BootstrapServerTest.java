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
package io.kyligence.kap.rest;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import org.apache.kylin.common.KylinConfig;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.session.web.http.CookieSerializer;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

@MetadataInfo(onlyProps = true)
class BootstrapServerTest {

    @InjectMocks
    BootstrapServer server = Mockito.spy(new BootstrapServer());

    @Test
    void testCookieSerializer() throws Exception {
        String metadataUrl = "test_docker@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://mysql:3306/kylin?useSSL=false,createDatabaseIfNotExist=true,username=root,password=root";
        String expectedName = "ce8a5bd5b23fe84d1894dce1bd9399364ba5af3c7a25f025442c92244c729804";

        KylinConfig.getInstanceFromEnv().setMetadataUrl(metadataUrl);
        CookieSerializer cookieSerializer = server.cookieSerializer();

        Class cookieClass = cookieSerializer.getClass();
        Field field = cookieClass.getDeclaredField("cookieName");
        field.setAccessible(true);
        Object cookieName = field.get(cookieSerializer);

        assertEquals(cookieName, expectedName);
    }

}
