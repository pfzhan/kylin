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
package io.kyligence.kap.rest.request;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.Getter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class ReloadTableContext {

    private Map<String, ReloadTableAffectedModelContext> removeAffectedModels = Maps.newHashMap();

    private Map<String, ReloadTableAffectedModelContext> changeTypeAffectedModels = Maps.newHashMap();

    private Set<String> favoriteQueries = Sets.newHashSet();

    private Set<String> addColumns = Sets.newHashSet();

    private Set<String> removeColumns = Sets.newHashSet();

    private Set<String> changeTypeColumns = Sets.newHashSet();

    private TableDesc tableDesc;

    private TableExtDesc tableExtDesc;

    @Getter(lazy = true)
    private final Set<String> removeColumnFullnames = initRemoveColumnFullnames();

    Set<String> initRemoveColumnFullnames() {
        return removeColumns.stream().map(col -> tableDesc.getName() + "." + col).collect(Collectors.toSet());
    }

}
