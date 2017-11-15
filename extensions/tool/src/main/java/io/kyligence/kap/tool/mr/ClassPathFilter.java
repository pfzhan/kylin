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

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ClassPathFilter {

    public static void main(String[] args) throws IOException {
        ArrayList<String> classPath = new ArrayList<>();
        String classpath = args[0];
        ArrayList<String> filters = new ArrayList<>(Arrays.asList(args));
        filters.remove(0);
        String[] split = classpath.split(":");
        if (split.length == 0) {
            System.out.println("no class path found!");
            System.exit(1);
        }
        for (String path : split) {
            if (path.endsWith("*")) {
                path = path.substring(0, path.length() - 1);
            }
            Files.walkFileTree(Paths.get(path), EnumSet.noneOf(FileVisitOption.class), 1,
                    new findJavaVisitor(classPath, filters));
        }
        Iterator<String> iter = classPath.iterator();
        StringBuilder sb = new StringBuilder();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext()) {
                sb.append(":");
            }
        }
        System.out.println(sb);
    }

    private static class findJavaVisitor extends SimpleFileVisitor<Path> {
        private List<String> filters;
        private ArrayList<String> classPath;

        public findJavaVisitor(ArrayList<String> classPath, List<String> filters) {
            this.classPath = classPath;
            this.filters = filters;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            Objects.requireNonNull(dir);
            Objects.requireNonNull(attrs);
            if (dir.getFileName().toString().endsWith("conf")) {
                classPath.add(dir.toString());
                return FileVisitResult.SKIP_SUBTREE;
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.getFileName().toString().endsWith("conf")) {
                classPath.add(file.toString());
                return FileVisitResult.CONTINUE;
            }
            if (Files.isSymbolicLink(file)) {
                try {
                    Files.walkFileTree(Files.readSymbolicLink(file), new findJavaVisitor(classPath, filters));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } else {
                if (!(file.getFileName().toString().endsWith(".jar")
                        || file.getFileName().toString().endsWith(".xml"))) {
                    return FileVisitResult.CONTINUE;
                }
                for (String filter : filters) {
                    if (file.getFileName().toString().contains(filter)) {
                        return FileVisitResult.CONTINUE;
                    }
                }
                classPath.add(file.toAbsolutePath().toString());
                // TODO Auto-generated method stub
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE;
        }
    }
}
