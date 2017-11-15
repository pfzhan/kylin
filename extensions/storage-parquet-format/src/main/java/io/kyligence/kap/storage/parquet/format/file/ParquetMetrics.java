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

package io.kyligence.kap.storage.parquet.format.file;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMetrics {
    public static final Logger logger = LoggerFactory.getLogger(ParquetMetrics.class);

    static ThreadLocal<ParquetMetrics> metric = new ThreadLocal<>();
    private long footerReadCnt;
    private long footerReadTime;
    private long footerReadTmp;

    private long pageReadOverallPageCnt;
    private long pageReadOverallCellCnt;
    private long pageReadOverallTime;
    private long pageReadOverallTmp;

    private long pageReadHeaderCnt;
    private long pageReadHeaderSeekTime;
    private long pageReadHeaderSeekTmp;
    private long pageReadHeaderStreamTime;
    private long pageReadHeaderStreamTmp;
    private long pageReadHeaderTime;
    private long pageReadHeaderTmp;

    private long pageReadIOAndDecompressRawIOBytes;
    private long pageReadIOAndDecompressBytes;
    private long pageReadIOAndDecompressTime;
    private long pageReadIOAndDecompressTmp;

    private long pageReadDecodeBytes;
    private long pageReadDecodeTime;
    private long pageReadDecodeTmp;

    private long groupReadTmp;
    private long groupReadTime;

    private long bufferReadTime;
    private long bufferReadTmp;

    public void groupReadStart() {
        groupReadTmp = System.nanoTime();
    }

    public void groupReadEnd() {
        groupReadTime += System.nanoTime() - groupReadTmp;
    }

    public void bufferReadStart() {
        bufferReadTmp = System.nanoTime();
    }

    public void bufferReadEnd() {
        bufferReadTime += System.nanoTime() - bufferReadTmp;
    }

    public static ParquetMetrics get() {
        ParquetMetrics metrics = metric.get();
        if (metrics == null) {
            ParquetMetrics parquetMetrics = new ParquetMetrics();
            metric.set(parquetMetrics);
            return parquetMetrics;
        }
        return metrics;
    }

    public void footerReadStart() {
        footerReadTmp = System.nanoTime();
    }

    public void footerReadEnd() {
        footerReadCnt++;
        footerReadTime += System.nanoTime() - footerReadTmp;
    }

    public void pageReadOverallStart() {
        pageReadOverallTmp = System.nanoTime();
    }

    public void pageReadOverallEnd(long cellCnt) {
        pageReadOverallPageCnt++;
        pageReadOverallCellCnt += cellCnt;
        pageReadOverallTime += System.nanoTime() - pageReadOverallTmp;
    }

    public void pageReadHeaderStart() {
        pageReadHeaderTmp = System.nanoTime();
    }

    public void pageReadHeaderEnd() {
        pageReadHeaderCnt++;
        pageReadHeaderTime += System.nanoTime() - pageReadHeaderTmp;
    }

    public void pageReadHeaderSeekStart() {
        pageReadHeaderSeekTmp = System.nanoTime();
    }

    public void pageReadHeaderSeekEnd() {
        pageReadHeaderSeekTime += System.nanoTime() - pageReadHeaderSeekTmp;
    }

    public void pageReadHeaderStreamStart() {
        pageReadHeaderStreamTmp = System.nanoTime();
    }

    public void pageReadHeaderStreamEnd() {
        pageReadHeaderStreamTime += System.nanoTime() - pageReadHeaderStreamTmp;
    }

    public void pageReadIOAndDecompressStart() {
        pageReadIOAndDecompressTmp = System.nanoTime();
    }

    public void pageReadIOAndDecompressEnd(long rawIOBytes, long decompressedBytes) {
        pageReadIOAndDecompressRawIOBytes += rawIOBytes;
        pageReadIOAndDecompressBytes += decompressedBytes;
        pageReadIOAndDecompressTime += System.nanoTime() - pageReadIOAndDecompressTmp;
    }

    public void pageReadDecodeStart() {
        pageReadDecodeTmp = System.nanoTime();
    }

    public void pageReadDecodeEnd(long decodedBytes) {
        pageReadDecodeBytes += decodedBytes;
        pageReadDecodeTime += System.nanoTime() - pageReadDecodeTmp;
    }

    public void print(PrintStream out) {
        logger.info("...footerReadCnt\t" + footerReadCnt);
        logger.info("...footerReadTime\t" + footerReadTime / 1000000);

        logger.info("...pageReadOverallPageCnt\t" + pageReadOverallPageCnt);
        logger.info("...pageReadOverallCellCnt\t" + pageReadOverallCellCnt);
        logger.info("...pageReadOverallTime\t" + pageReadOverallTime / 1000000);

        logger.info("...pageReadHeaderCnt\t" + pageReadHeaderCnt);
        logger.info("...pageReadHeaderTime\t" + pageReadHeaderTime / 1000000);
        logger.info("...pageReadHeaderSeekTime\t" + pageReadHeaderSeekTime / 1000000);
        logger.info("...pageReadHeaderStreamTime\t" + pageReadHeaderStreamTime / 1000000);

        logger.info("...pageReadIOAndDecompressRawIOBytes\t" + pageReadIOAndDecompressRawIOBytes);
        logger.info("...pageReadIOAndDecompressBytes\t" + pageReadIOAndDecompressBytes);
        logger.info("...pageReadIOAndDecompressTime\t" + pageReadIOAndDecompressTime / 1000000);

        logger.info("...pageReadDecodeBytes\t" + pageReadDecodeBytes);
        logger.info("...pageReadDecodeTime\t" + pageReadDecodeTime / 1000000);
        logger.info("...groupReadTime\t" + groupReadTime / 1000000);
        logger.info("...bufferReadTime\t" + bufferReadTime / 1000000);
        logger.info("...totalTime\t"
                + (groupReadTime + bufferReadTime + pageReadDecodeTime + pageReadIOAndDecompressTime) / 1000000);
        metric.set(null);
    }
}
