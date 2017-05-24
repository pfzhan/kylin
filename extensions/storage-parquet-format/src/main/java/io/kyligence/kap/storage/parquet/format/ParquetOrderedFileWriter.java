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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

public abstract class ParquetOrderedFileWriter<K, V> extends RecordWriter<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetOrderedFileWriter.class);

    /**
     * Init writer is always null, init it before use
     */
    protected ParquetRawWriter writer = null;

    /**
     * create parquet file writer
     * @return new parquet writer
     */
    abstract protected ParquetRawWriter newWriter() throws IOException, InterruptedException;

    abstract protected void cleanWriter() throws IOException;

    /**
     * write data to parquet file
     * @param key
     * @param value
     */
    abstract protected void writeData(K key, V value) throws IOException;

    /**
     * Fresh parquet file writer.
     * If there's no writer, create one.
     * Otherwise close the
     * @return new parquet writer
     */
    abstract protected void freshWriter(K key, V value) throws IOException, InterruptedException;

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
        freshWriter(key, value);
        writeData(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            cleanWriter();
        } catch (IOException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (RuntimeException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (Error ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        }
    }

    abstract protected Path getOutputPath();
}
