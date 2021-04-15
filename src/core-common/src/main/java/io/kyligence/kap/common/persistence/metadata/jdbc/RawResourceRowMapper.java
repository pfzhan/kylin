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
package io.kyligence.kap.common.persistence.metadata.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.SneakyThrows;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.CompressionUtils;
import org.springframework.jdbc.core.RowMapper;

import com.google.common.io.ByteStreams;

import lombok.val;

public class RawResourceRowMapper implements RowMapper<RawResource> {
    @SneakyThrows
    @Override
    public RawResource mapRow(ResultSet rs, int rowNum) throws SQLException {
        val resPath = rs.getString(1);
        val content = CompressionUtils.decompress(rs.getBytes(2));
        val ts = rs.getLong(3);
        val mvcc = rs.getLong(4);
        return new RawResource(resPath, ByteStreams.asByteSource(content), ts, mvcc);
    }
}