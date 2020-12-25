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
package io.kyligence.api.catalog;

import io.kyligence.api.annotation.InterfaceStability.Evolving;

import java.util.Map;
import java.util.Objects;

@Evolving
public class Database {

    private String name;
    private String description;
    private String locationUri;
    private Map<String, String> parameters; // properties associated with the database

    public Database(
            String name,
            String description,
            String locationUri,
            Map<String, String> parameters) {
        this.name = name;
        this.description = description;
        this.locationUri = locationUri;
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getLocationUri() {
        return locationUri;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Database database = (Database) o;
        return Objects.equals(name, database.name)
                && Objects.equals(description, database.description)
                && Objects.equals(locationUri, database.locationUri)
                && Objects.equals(parameters, database.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, locationUri, parameters);
    }

    @Override
    public String toString() {
        return "Database{"
                + "name='" + name + '\''
                + ", description='" + description + '\''
                + ", locationUri='" + locationUri + '\''
                + ", parameters=" + parameters
                + '}';
    }


}
