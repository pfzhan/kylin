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
package io.kyligence.kap.tool.obf;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class MappingRecorder implements Closeable {
    private boolean isClosed = false;
    private File output;

    public MappingRecorder(File output) {
        this.output = output;
    }

    private Map<String, Map<String, String>> mapping = Maps.newConcurrentMap();

    public String addMapping(String catalog, String from, String to) {
        Preconditions.checkState(!isClosed);

        if (!mapping.containsKey(catalog)) {
            mapping.put(catalog, Maps.newConcurrentMap());
        }

        mapping.get(catalog).putIfAbsent(from, to);

        return to;
    }

    public String addMapping(ObfCatalog catalog, String from) {
        return addMapping(catalog.value, from, catalog.obf(from));
    }

    public Map<String, Map<String, String>> getMapping() {
        return mapping;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            store();
            isClosed = true;
            mapping = null;
        }
    }

    public void store() throws IOException {
        if (output == null) {
            return;
        }

        try (OutputStream os = new FileOutputStream(output)) {
            JsonUtil.writeValue(os, mapping);
            os.flush();
        }
    }
}
