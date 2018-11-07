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

package io.kyligence.kap.storage.parquet.format;

import com.google.common.collect.Lists;
import io.kyligence.kap.storage.parquet.format.file.AbstractParquetFormatTest;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore("jdk 1.8")
public class ParquetTarballFileInputFormatTest extends AbstractParquetFormatTest {
    public ParquetTarballFileInputFormatTest() throws IOException {
        type = new MessageType("test",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "key"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 50, "m1"));
    }

    // value buffer is 1024 * 1024
    @Test
    public void testLargeValueGood() throws IOException, InterruptedException {
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path).setType(type).build();
        writer.writeRow(Lists.newArrayList(new byte[100], new byte[100 * 1024 * 1024]));
        writer.close();
        ParquetTarballFileInputFormat.ParquetTarballFileReader tarballReader = new ParquetTarballFileInputFormat.ParquetTarballFileReader();
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(new Configuration()).build();
        tarballReader.setReader(bundleReader);
        tarballReader.setBinaryFilter(null);
        tarballReader.setDefaultValue(new byte[ParquetFormatConstants.KYLIN_DEFAULT_GT_MAX_LENGTH]);
        tarballReader.setReadStrategy(ParquetTarballFileInputFormat.ParquetTarballFileReader.ReadStrategy.KV);
        tarballReader.nextKeyValue();
    }

    // value buffer is 1024
    @Test
    public void testLargeValueBad() throws IOException, InterruptedException{
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path).setType(type).build();
        writer.writeRow(Lists.newArrayList(new byte[100], new byte[100 * 1024 * 1024]));
        writer.close();
        ParquetTarballFileInputFormat.ParquetTarballFileReader tarballReader = new ParquetTarballFileInputFormat.ParquetTarballFileReader();
        ParquetBundleReader bundleReader = new ParquetBundleReader.Builder().setPath(path).setConf(new Configuration()).build();
        tarballReader.setReader(bundleReader);
        tarballReader.setBinaryFilter(null);
        tarballReader.setDefaultValue(new byte[1024]);
        tarballReader.setReadStrategy(ParquetTarballFileInputFormat.ParquetTarballFileReader.ReadStrategy.KV);
        try {
            tarballReader.nextKeyValue();
        } catch (IllegalStateException ex) {
            if (ex.getMessage().contains("Measures taking too much space!")) {
                return;
            }
        }
        Assert.fail();
    }
}
