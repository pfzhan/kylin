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

package io.kyligence.kap.metadata.jar;

import java.io.Serializable;
import java.util.Locale;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Data
public class JarInfo extends RootPersistentEntity implements Serializable {

    @JsonProperty("project")
    private String project;

    @JsonProperty("jar_name")
    private String jarName;

    @JsonProperty("jar_path")
    private String jarPath;

    @JsonProperty("jar_type")
    private JarTypeEnum jarType;

    public JarInfo() {
    }

    public JarInfo(String project, String jarName, String jarPath, JarTypeEnum jarType) {
        this.project = project;
        this.jarName = jarName;
        this.jarPath = jarPath;
        this.jarType = jarType;
    }

    @Override
    public String resourceName() {
        if (this.jarType == null || this.jarName == null) {
            return null;
        }
        return String.format(Locale.ROOT, "%s_%s", this.jarType, this.jarName);
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(resourceName(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return String.format(Locale.ROOT, "/%s%s/%s%s", project, ResourceStore.JAR_RESOURCE_ROOT, name,
                MetadataConstants.FILE_SURFIX);
    }
}
