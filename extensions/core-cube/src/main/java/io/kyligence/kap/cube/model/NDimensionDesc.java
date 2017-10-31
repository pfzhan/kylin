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

package io.kyligence.kap.cube.model;

import java.io.Serializable;

import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.common.obf.IKeep;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class NDimensionDesc implements Serializable, IKeep {

    @JsonProperty("id")
    private int id;

    @JsonProperty("encoding")
    private NEncodingDesc encoding;

    public NDimensionDesc() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public NEncodingDesc getEncoding() {
        encoding.init();
        return encoding;
    }

    public void setEncoding(NEncodingDesc encoding) {
        this.encoding = encoding;
    }

    @JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
    public static class NEncodingDesc implements Serializable, IKeep {
        @JsonProperty("name")
        private String name;
        @JsonProperty("version")
        private int version;

        private String encodingName;
        private String[] encodingArgs;

        public NEncodingDesc() {
        }

        public void init() {
            Object[] encodingConf = DimensionEncoding.parseEncodingConf(this.name);
            encodingName = (String) encodingConf[0];
            encodingArgs = (String[]) encodingConf[1];
            if (!DimensionEncodingFactory.isValidEncoding(this.encodingName))
                throw new IllegalArgumentException("Not supported row key col encoding: '" + this.name + "'");
            //TODO: refer RowKeyColDesc.init()
        }

        public String getEncodingName() {
            return encodingName;
        }

        public String[] getEncodingArgs() {
            return encodingArgs;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }
    }
}
