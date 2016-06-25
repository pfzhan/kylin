/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.cube.index.pinot;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.index.pinot.util.MmapUtils;
import io.kyligence.kap.cube.index.pinot.util.SizeUtil;
import me.lemire.integercompression.BitPacking;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 *
 * Represents a datatable where each col contains values that can be represented
 * using a fix set of bits.
 */
public class FixedBitSingleValueWriter implements SingleColumnSingleValueWriter {
    private static final Logger logger = LoggerFactory.getLogger(FixedBitSingleValueWriter.class);

    private static final int BUFFED_ROW_NUM = 4000000;
    private static final int HEADER_BYTES = V1Constants.Idx.SV_COLUMN_IDX_FILE_HEADER_BYTES;

    private int bufferBytes = -1;
    private int reallocateCounter = 0;
    private ByteBuffer byteBuffer;
    private RandomAccessFile raf;
    private int maxValue;
    private int minValue;
    private int currentRow = -1;
    private int maxRow = -1;
    private int numBits;
    private int compressedSize;
    private int uncompressedSize;
    private int[] uncompressedData;
    private int[] compressedData;
    boolean ownsByteBuffer;
    boolean ownUncompressedData;
    boolean isMmap;
    File idxFile;

    private void init(File file, int numBits, boolean signed) throws Exception {
        this.idxFile = file;
        this.raf = new RandomAccessFile(idxFile, "rw");

        init(numBits, signed);
        this.bufferBytes = SizeUtil.computeBytesRequired(BUFFED_ROW_NUM, this.numBits, uncompressedSize);

        createBuffer();
    }

    public FixedBitSingleValueWriter(File file, int numBits) throws Exception {
        init(file, numBits, false);
    }

    public FixedBitSingleValueWriter(File file, int numBits, boolean hasNegativeValues) throws Exception {
        init(file, numBits, hasNegativeValues);
    }

    public FixedBitSingleValueWriter(ByteBuffer byteBuffer, int numBits) throws Exception {
        this.byteBuffer = byteBuffer;
        init(numBits, false);
    }

    public FixedBitSingleValueWriter(ByteBuffer byteBuffer, int numBits, boolean hasNegativeValues) throws Exception {
        this.byteBuffer = byteBuffer;
        init(numBits, hasNegativeValues);
    }

    private void init(int numBits, boolean signed) throws Exception {
        int max = (int) Math.pow(2, numBits);
        this.maxValue = max - 1;

        // additional bit for sign
        if (signed) {
            this.minValue = -1 * maxValue;
            this.numBits = numBits + 1;
        } else {
            this.minValue = 0;
            this.numBits = numBits;
        }
        uncompressedSize = SizeUtil.BIT_UNPACK_BATCH_SIZE;
        compressedSize = numBits;
        uncompressedData = new int[uncompressedSize];
        compressedData = new int[compressedSize];
    }

    private void createBuffer() throws IOException {
        logger.info("Creating byteBuffer of size:{}Bytes to store values of bits:{}", bufferBytes, numBits);
        byteBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, HEADER_BYTES + bufferBytes * reallocateCounter, bufferBytes, idxFile, this.getClass().getSimpleName() + " byteBuffer");
        isMmap = true;
        ownsByteBuffer = true;
        byteBuffer.position(0);
    }

    public boolean open() {
        return true;
    }

    /**
     * @param row
     * @param val
     */
    public void setInt(int row, int val) {
        try {
            assert val >= minValue && val <= maxValue && row == currentRow + 1;

            int index = row % uncompressedSize;
            uncompressedData[index] = val;
            ownUncompressedData = true;
            if (index == uncompressedSize - 1) {
                compressAndFlush();
            }
            currentRow = row;
            if (currentRow > maxRow) {
                maxRow = currentRow;
            }
        } catch (Exception e) {
            logger.error("Failed to set row:{} val:{} ", row, val, e);
            throw new RuntimeException(e);
        }
    }

    private void compressAndFlush() {
        BitPacking.fastpack(uncompressedData, 0, compressedData, 0, numBits);
        for (int i = 0; i < compressedSize; i++) {
            byteBuffer.putInt(compressedData[i]);
            if (idxFile != null && byteBuffer.remaining() == 0) {
                reallocateBuffer();
            }
        }
        Arrays.fill(uncompressedData, 0);
        ownUncompressedData = false;
    }

    private void reallocateBuffer() {
        try {
            MmapUtils.unloadByteBuffer(byteBuffer);
            reallocateCounter++;
            createBuffer();
        } catch (Exception e) {
            logger.error("failed to reallocate buffer, reallocateCounter={}", reallocateCounter);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (ownUncompressedData) {
            compressAndFlush();
        }
        writeHeader();
        if (ownsByteBuffer) {
            MmapUtils.unloadByteBuffer(byteBuffer);
            byteBuffer = null;

            if (isMmap) {
                IOUtils.closeQuietly(raf);
                raf = null;
            }
        }
    }

    private void writeHeader() throws IOException {
        if (HEADER_BYTES > 0) {
            ByteBuffer headerBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, 0, HEADER_BYTES, idxFile, this.getClass().getSimpleName() + " byteBuffer");
            headerBuffer.putInt(maxRow + 1);

            MmapUtils.unloadByteBuffer(headerBuffer);
        }
    }

    @Override
    public void setChar(int row, char ch) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setShort(int row, short s) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setLong(int row, long l) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setFloat(int row, float f) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setDouble(int row, double d) {
        // TODO Auto-generated method stub
    }

    @Override
    public void setString(int row, String string) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void setBytes(int row, byte[] bytes) {
        // TODO Auto-generated method stub
    }

}
