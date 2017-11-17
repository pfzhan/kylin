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

package io.kyligence.kap.tool.mr;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ClassPathFilter {

    public static void main(String[] args) throws IOException {
        String classpath = args[0];
        ArrayList<String> filters = new ArrayList<>(Arrays.asList(args));
        filters.remove(0);
        String[] split = classpath.split(":");
        if (split.length == 0) {
            System.out.println("no class path found!");
            System.exit(1);
        }
        List<File> fileList = new ArrayList<>();
        for (String path : split) {
            if (path.endsWith("*")) {
                path = path.substring(0, path.length() - 1);
            }
            Path filePath = Paths.get(path);
            File file = filePath.toFile();
            if (file.isDirectory()) {
                if (filePath.getFileName().toString().endsWith("conf")) {
                    fileList.add(filePath.toFile());
                } else {
                    File[] childrenFiles = file.listFiles();
                    for (File childrenFile : childrenFiles) {
                        filterFile(filters, fileList, childrenFile);

                    }
                }
            } else {
                filterFile(filters, fileList, file);
            }
        }
        Iterator<File> iter = fileList.iterator();
        StringBuilder sb = new StringBuilder();
        while (iter.hasNext()) {
            sb.append(iter.next().getCanonicalPath());
            if (iter.hasNext()) {
                sb.append(":");
            }
        }
        System.out.println(sb);
    }

    private static void filterFile(ArrayList<String> filters, List<File> fileList, File childrenFile) {
        if (childrenFile.getName().endsWith("jar")) {
            boolean needFilter = false;
            for (String filter : filters) {
                if (childrenFile.getName().contains(filter)) {
                    needFilter = true;
                }
                if (!needFilter) {
                    fileList.add(childrenFile);
                }
            }
        }
    }
}
