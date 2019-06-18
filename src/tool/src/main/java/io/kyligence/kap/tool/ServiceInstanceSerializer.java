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
package io.kyligence.kap.tool;

import java.util.Map;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;


public class ServiceInstanceSerializer<T> extends JsonInstanceSerializer<T> {

    private ObjectMapper mapper = new ObjectMapper();

    public ServiceInstanceSerializer(Class<T> payloadClass) {
        super(payloadClass);
    }

    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        String content = new String(bytes);
        Map map = mapper.readValue(content, Map.class);
        return castToServiceInstance(map);
    }

    private ServiceInstance<T> castToServiceInstance(Map map) {
        String name = (String) map.getOrDefault("name", "");
        String id = (String) map.getOrDefault("id", "");
        String address = (String) map.getOrDefault("address", null);
        Integer port = (Integer) map.getOrDefault("port", null);

        return new ServiceInstance(name, id, address, port, null, null, 0L, ServiceType.DYNAMIC, null);
    }
}
