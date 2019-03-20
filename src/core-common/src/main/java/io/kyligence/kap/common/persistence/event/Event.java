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

package io.kyligence.kap.common.persistence.event;

import java.io.Serializable;

import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.AuditLog;
import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class Event implements Serializable, IKeep {

    private String key;

    public static Event fromLog(AuditLog log) {
        Event event;
        if (log.getByteSource() == null) {
            event = new ResourceDeleteEvent(log.getResPath());
        } else {
            event = new ResourceCreateOrUpdateEvent(
                    new RawResource(log.getResPath(), log.getByteSource(), log.getTimestamp(), log.getMvcc()));
        }
        if (log.getResPath().startsWith(ResourceStore.PROJECT_ROOT)) {
            event.setKey(log.getResPath().substring(ResourceStore.PROJECT_ROOT.length() + 1).replace(".json", ""));
        } else {
            event.setKey(log.getResPath().split("/")[1]);
        }
        return event;
    }

}
