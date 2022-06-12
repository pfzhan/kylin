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

package io.kyligence.kap.rest.service;

import com.clearspring.analytics.util.Lists;
import lombok.val;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class XLSXExcelWriter {

    private static final Logger logger = LoggerFactory.getLogger("query");

    public void writeData(FileStatus[] fileStatuses, Sheet sheet) {
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.getPath().getName().startsWith("_")) {
                if (fileStatus.getPath().getName().endsWith("parquet")) {
                    writeDataByParquet(fileStatus, sheet);
                } else if (fileStatus.getPath().getName().endsWith("xlsx")) {
                    writeDataByXlsx(fileStatus, sheet);
                } else {
                    writeDataByCsv(fileStatus, sheet);
                }
            }
        }
    }

    private void writeDataByXlsx(FileStatus f, Sheet sheet) {
        boolean createTempFileStatus = false;
        File file = new File("temp.xlsx");
        try {
            createTempFileStatus = file.createNewFile();
            FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
            fileSystem.copyToLocalFile(f.getPath(), new Path(file.getPath()));
        } catch (Exception e) {
            logger.error("Export excel writeDataByXlsx create exception f:{} createTempFileStatus:{} ",
                    f.getPath(), createTempFileStatus, e);
        }
        try (InputStream is = new FileInputStream(file.getAbsolutePath());
             XSSFWorkbook sheets = new XSSFWorkbook(is)) {
            final AtomicInteger offset = new AtomicInteger(sheet.getPhysicalNumberOfRows());
            XSSFSheet sheetAt = sheets.getSheetAt(0);
            for (int i = 0; i < sheetAt.getPhysicalNumberOfRows(); i++) {
                XSSFRow row = sheetAt.getRow(i);
                org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(offset.get());
                offset.incrementAndGet();
                for (int index = 0; index < row.getPhysicalNumberOfCells(); index++) {
                    XSSFCell cell = row.getCell(index);
                    excelRow.createCell(index).setCellValue(getString(cell));
                }
            }
            Files.delete(file.toPath());
        } catch (Exception e) {
            logger.error("Export excel writeDataByXlsx handler exception f:{} createTempFileStatus:{} ",
                    f.getPath(), createTempFileStatus, e);
        }
    }

    private static String getString(XSSFCell xssfCell) {
        if (xssfCell == null) {
            return "";
        }
        if (xssfCell.getCellType() == CellType.NUMERIC) {
            return String.valueOf(xssfCell.getNumericCellValue());
        } else if (xssfCell.getCellType() == CellType.BOOLEAN) {
            return String.valueOf(xssfCell.getBooleanCellValue());
        } else {
            return xssfCell.getStringCellValue();
        }
    }

    private void writeDataByParquet(FileStatus fileStatus, Sheet sheet) {
        final AtomicInteger offset = new AtomicInteger(sheet.getPhysicalNumberOfRows());
        List<org.apache.spark.sql.Row> rowList = SparderEnv.getSparkSession().read()
                .parquet(fileStatus.getPath().toString()).collectAsList();
        rowList.stream().forEach(row -> {
            org.apache.poi.ss.usermodel.Row excelRow = sheet.createRow(offset.get());
            offset.incrementAndGet();
            val list = row.toSeq().toList();
            for (int i = 0; i < list.size(); i++) {
                Object cell = list.apply(i);
                String column = cell == null ? "" : cell.toString();
                excelRow.createCell(i).setCellValue(column);
            }
        });
    }

    public void writeDataByCsv(FileStatus fileStatus, Sheet sheet) {
        FileSystem fileSystem = AsyncQueryUtil.getFileSystem();
        List<String> rowResults = Lists.newArrayList();
        List<String[]> results = Lists.newArrayList();
        final AtomicInteger offset = new AtomicInteger(sheet.getPhysicalNumberOfRows());
        try (FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath())) {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            rowResults.addAll(Lists.newArrayList(bufferedReader.lines().collect(Collectors.toList())));
            for (String row : rowResults) {
                results.add(row.split(SparderEnv.getSeparator()));
            }
            for (int i = 0; i < results.size(); i++) {
                Row row = sheet.createRow(offset.get());
                offset.incrementAndGet();
                String[] rowValues = results.get(i);
                for (int j = 0; j < rowValues.length; j++) {
                    row.createCell(j).setCellValue(rowValues[j]);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to download asyncQueryResult xlsxExcel by csv", e);
        }
    }
}
