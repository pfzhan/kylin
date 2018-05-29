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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.metrics.lib.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metrics.lib.Record;

import com.google.common.collect.Maps;

public class RecordEvent implements Record, Map<String, Object>, Serializable {

    private static final ThreadLocal<ByteArrayOutputStream> _localBaos = new ThreadLocal<ByteArrayOutputStream>();

    static String localHostname;
    static {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            localHostname = addr.getHostName() + ":" + addr.getHostAddress();
        } catch (UnknownHostException e) {
            localHostname = "Unknown";
        }
    }

    private final Map<String, Object> backingMap;

    private RecordEvent(Map<String, Object> map) {
        this.backingMap = map;
    }

    public RecordEvent(String eventType) {
        this(eventType, localHostname);
    }

    public RecordEvent(String eventType, long time) {
        this(eventType, localHostname, time);
    }

    public RecordEvent(String eventType, String host) {
        this(eventType, host, System.currentTimeMillis());
    }

    public RecordEvent(String eventType, String host, long time) {
        this(null, eventType, host, time);
    }

    /**
     *
     * @param map
     * @param eventType     mandatory   with null check
     * @param host          mandatory   without null check
     * @param time          mandatory   with null check
     */
    public RecordEvent(Map<String, Object> map, String eventType, String host, long time) {
        backingMap = map != null ? map : Maps.<String, Object> newHashMap();
        setEventType(eventType);
        setHost(host);
        setTime(time);
    }

    public String getEventType() {
        return (String) get(RecordReserveKeyEnum.TYPE.toString());
    }

    private void setEventType(String eventType) {
        if (eventType == null) {
            throw new IllegalArgumentException("EventType cannot be null.");
        }
        put(RecordReserveKeyEnum.TYPE.toString(), eventType);
    }

    public String getHost() {
        return (String) get(RecordReserveKeyEnum.HOST.toString());
    }

    private void setHost(String host) {
        put(RecordReserveKeyEnum.HOST.toString(), host);
    }

    public Long getTime() {
        return (Long) get(RecordReserveKeyEnum.TIME.toString());
    }

    private void setTime(Long time) {
        if (time == null) {
            throw new IllegalArgumentException("Time cannot be null.");
        }
        put(RecordReserveKeyEnum.TIME.toString(), time);
    }

    public void resetTime() {
        setTime(System.currentTimeMillis());
    }

    public String getID() {
        return (String) get(RecordReserveKeyEnum.ID.toString());
    }

    public void setID(String id) {
        put(RecordReserveKeyEnum.ID.toString(), id);
    }

    @Override
    public void clear() {
        backingMap.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return backingMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return backingMap.containsValue(value);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return backingMap.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) || backingMap.equals(o);
    }

    @Override
    public Object get(Object key) {
        return backingMap.get(key);
    }

    @Override
    public int hashCode() {
        return backingMap.hashCode();
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public Set<String> keySet() {
        return backingMap.keySet();
    }

    @Override
    public Object put(String key, Object value) {
        return backingMap.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> t) {
        backingMap.putAll(t);
    }

    @Override
    public Object remove(Object key) {
        return backingMap.remove(key);
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public String toString() {
        return backingMap.toString();
    }

    @Override
    public Collection<Object> values() {
        return backingMap.values();
    }

    @Override
    public String getType() {
        return getEventType();
    }

    @Override
    public byte[] getKey() {
        return (getHost() + "-" + getTime() + "-" + getID()).getBytes();
    }

    @Override
    /**
     * Event type and time does not belong to value part
     */
    public Map<String, Object> getValueRaw() {
        Map<String, Object> cloneMap = Maps.newHashMap(backingMap);
        cloneMap.remove(RecordReserveKeyEnum.TYPE.toString());
        return cloneMap;
    }

    @Override
    /**
     * Event type does not belong to value part, it's for classification
     */
    public byte[] getValue() {
        try {
            ByteArrayOutputStream baos = _localBaos.get();
            if (baos == null) {
                baos = new ByteArrayOutputStream();
                _localBaos.set(baos);
            }
            baos.reset();
            JsonUtil.writeValue(baos, getValueRaw());
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);//in mem, should not happen
        }
    }

    @Override
    public RecordEvent clone() {
        Map<String, Object> cloneMap = Maps.newHashMap();
        cloneMap.putAll(backingMap);
        return new RecordEvent(cloneMap);
    }

    public enum RecordReserveKeyEnum {
        TYPE("EVENT_TYPE"), ID("EVENT_ID"), HOST("HOST"), TIME("KTIMESTAMP");

        private final String reserveKey;

        private RecordReserveKeyEnum(String key) {
            this.reserveKey = key;
        }

        @Override
        public String toString() {
            return reserveKey;
        }

        public RecordReserveKeyEnum getByKey(String key) {
            for (RecordReserveKeyEnum reserveKey : RecordReserveKeyEnum.values()) {
                if (reserveKey.reserveKey == key) {
                    return reserveKey;
                }
            }

            return null;
        }
    }
}
