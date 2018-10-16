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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metric.MetricWriterStrategy;

public class ParquetMetrics {
    public static final Logger logger = LoggerFactory.getLogger(ParquetMetrics.class);

    static ThreadLocal<ParquetMetrics> metric = new ThreadLocal<>();
    static ThreadLocal<ParquetMetrics> executorMetric = new ThreadLocal<>();
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
            executorMetric.set(parquetMetrics);
            return parquetMetrics;
        }
        return metrics;
    }

    public static ParquetMetrics getExecutorMetric() {
        ParquetMetrics metrics = executorMetric.get();
        if (metrics == null) {
            ParquetMetrics parquetMetrics = new ParquetMetrics();
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

    public void reset() {
        metric.set(null);
    }

    public String summary() {
        StringBuilder sb = new StringBuilder();
        Map<String, Long> metric = getMetric();
        sb.append("PARQUET METRICS: \n");
        for (String k : metric.keySet()) {
            sb.append("...").append(k).append("\t").append(metric.get(k)).append("\n");
        }
        return sb.toString();
    }

    public void reportToWriter(String host, String identifier) {
        try {
            Map<String, String> tags = new HashMap<>();
            tags.put("host", host);
            tags.put("identifier", identifier);

            Map<String, Object> fields = new HashMap<>();
            fields.putAll(getMetric());

            MetricWriterStrategy writer = MetricWriterStrategy.INSTANCE;
            writer.write("KE_METRIC", "parquet_metric", tags, fields);
        } catch (Throwable th) {
            logger.error("Metric write error.", th);
        }
        executorMetric.set(null);
    }

    private Map<String, Long> getMetric() {
        Map<String, Long> metric = new HashMap<>();
        metric.put("footerReadCnt", footerReadCnt);
        metric.put("footerReadTime", footerReadTime / 1000000);

        metric.put("pageReadHeaderCnt", pageReadHeaderCnt);
        metric.put("pageReadHeaderTime", pageReadHeaderTime / 1000000);
        metric.put("pageReadHeaderSeekTime", pageReadHeaderSeekTime / 1000000);
        metric.put("pageReadHeaderStreamTime", pageReadHeaderStreamTime / 1000000);

        metric.put("pageReadIOAndDecompressRawIOBytes", pageReadIOAndDecompressRawIOBytes);
        metric.put("pageReadIOAndDecompressBytes", pageReadIOAndDecompressBytes);
        metric.put("pageReadIOAndDecompressTime", pageReadIOAndDecompressTime / 1000000);

        // for sparder
        metric.put("pageReadDecodeBytes", pageReadDecodeBytes);
        metric.put("pageReadDecodeTime", pageReadDecodeTime / 1000000);
        metric.put("groupReadTime", groupReadTime / 1000000);
        metric.put("bufferReadTime", bufferReadTime / 1000000);

        metric.put("pageReadOverallPageCnt", pageReadOverallPageCnt);
        metric.put("pageReadOverallCellCnt", pageReadOverallCellCnt);
        metric.put("pageReadOverallTime", pageReadOverallTime / 1000000);

        metric.put("totalTime", (footerReadTime + pageReadHeaderTime + groupReadTime + bufferReadTime + pageReadDecodeTime + pageReadIOAndDecompressTime) / 1000000);
        return metric;
    }
}
