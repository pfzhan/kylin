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

package io.kyligence.kap.storage.parquet.cube.spark.rpc.sparder;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.kylin.gridtable.GTInfo;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.execution.utils.HexUtils;
 @Deprecated
public class KylinCast implements UDF2<Object, Integer, Object> {
    GTInfo gtInfo;
    String gtinfoStr;

    public KylinCast(String gtinfoStr) {
        this.gtinfoStr = gtinfoStr;
    }

    @Override
    public Object call(Object bytes, Integer col) throws Exception {
        if (gtInfo == null) {
            gtInfo = GTInfo.serializer.deserialize(ByteBuffer.wrap(HexUtils.toBytes(gtinfoStr)));
        }
        byte[] bytes1 = (byte[]) bytes;
        if(bytes==null) {
            return 0L;
        }
        Object o = gtInfo.getCodeSystem().decodeColumnValue(col, ByteBuffer.wrap(bytes1));
        if (o instanceof BigDecimal) {
            ByteBuffer allocate = ByteBuffer.allocate(1000);
            gtInfo.getCodeSystem().encodeColumnValue(col, o, allocate);
            //            System.out.println("cast"+ o);
            System.out.println("cast byte " + allocate.position());
        }
        return o;
    }

}
