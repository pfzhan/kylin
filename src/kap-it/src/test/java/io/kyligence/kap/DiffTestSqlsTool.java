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

package io.kyligence.kap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.query.util.CommentParser;

public class DiffTestSqlsTool {
    private final static String NEWTEN_DIRECTORY = "./src/test/resources/query/";

    private final List<String> KE3_x_DIRECTORY = Lists
            .newArrayList("/your/KE/code/directory/kap-it/src/test/resources/query/");

    @Test
    @Ignore
    public void test() {
        try {
            Map<String, String> newtenSqls = formatSql(getAllSqlFiles(NEWTEN_DIRECTORY));
            Map<String, String> keSqls = Maps.newHashMap();

            for (String dir : KE3_x_DIRECTORY) {
                keSqls.putAll(formatSql(getAllSqlFiles(dir)));
            }

            List<String> diffSqlPath = Lists.newArrayList();
            for (Map.Entry<String, String> entry : keSqls.entrySet()) {
                if (newtenSqls.get(entry.getKey()) == null) {
                    diffSqlPath.add(entry.getValue());
                }
            }
            Collections.sort(diffSqlPath);
            diffSqlPath.forEach(path -> System.out.println(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Path> getAllSqlFiles(String path) {
        try {
            return Files.walk(Paths.get(path))
                    .filter(file -> Files.isRegularFile(file) && !(file.toFile().getAbsolutePath().endsWith(".xml")
                            || file.toFile().getAbsolutePath().endsWith(".stats")
                            || file.toFile().getAbsolutePath().endsWith(".expected")))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Lists.newArrayList();
        }
    }

    private static Map<String, String> formatSql(List<Path> sqlFiles) throws Exception {
        Map<String, String> formatedSql2Path = Maps.newHashMap();
        for (Path path : sqlFiles) {
            String originSQl = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            CommentParser commentParser = new CommentParser(originSQl);
            String convertedSql = commentParser.Input();
            formatedSql2Path.put(convertedSql.replaceAll("\\s", ""), path.toFile().getAbsolutePath());
        }
        return formatedSql2Path;
    }

}
