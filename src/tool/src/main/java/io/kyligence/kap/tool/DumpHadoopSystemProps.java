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
package io.kyligence.kap.tool;

import io.kyligence.kap.common.obf.IKeep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;

public class DumpHadoopSystemProps implements IKeep {
    private static final String NO_PARAMETER = UUID.randomUUID().toString();
    private static final String UTF8 = "UTF-8";

    public static void main(String[] args) throws Exception {
        File tmp = File.createTempFile("kylin-env-diff-", ".sh");

        String filters = NO_PARAMETER;
        switch (args.length) {
            case 0:
                break;
            case 1:
                filters = args[0];
                break;
            default:
                System.out.println("Usages: DumpHadoopSystemProps 'filter1 filter2 filter3'");
                System.exit(1);
        }

        String[] cmd = new String[] { "hadoop", Inner.class.getName(), tmp.getAbsolutePath(), filters };
        Process proc = Runtime.getRuntime().exec(cmd);
        int code = proc.waitFor();
        if (code != 0)
            throw new IllegalStateException("Failed to execute: " + StringUtils.join(Arrays.asList(cmd), " "));

        TreeMap<String, String> propsDiff = diffSystemProps(tmp.getAbsolutePath() + ".props");
        TreeMap<String, String> envsDiff = diffSystemEnvs(tmp.getAbsolutePath() + ".envs");
        output(propsDiff, envsDiff, tmp);

        System.out.println(tmp.getAbsolutePath());
    }

    private static void output(TreeMap<String, String> propsDiff, TreeMap<String, String> envsDiff, File tmp)
            throws IOException {

        try (PrintWriter out = new PrintWriter(tmp, UTF8)) {

            for (Map.Entry<String, String> e : envsDiff.entrySet()) {
                out.println("export " + e.getKey() + "=" + doubleQuote(e.getValue()));
            }

            out.print("export kylin_hadoop_opts=\"");
            for (Map.Entry<String, String> e : propsDiff.entrySet()) {
                out.print(" -D" + e.getKey() + "=" + singleQuote(e.getValue()) + " ");
            }
            out.println("\"");

            out.println("rm -f " + tmp.getAbsolutePath());
        }
    }

    private static String doubleQuote(String s) {
        if (s.contains(" "))
            return "\"" + s + "\"";
        else
            return s;
    }

    private static String singleQuote(String s) {
        if (s.contains(" "))
            return "'" + s + "'";
        else
            return s;
    }

    private static TreeMap<String, String> diffSystemProps(String inPath) throws IOException {
        return diff(readAndDelete(inPath), getSystemProps());
    }

    private static TreeMap<String, String> diffSystemEnvs(String inPath) throws IOException {
        return diff(readAndDelete(inPath), getSystemEnvs());
    }

    private static TreeMap<String, String> diff(TreeMap<String, String> enhanced, TreeMap<String, String> orig) {
        TreeMap<String, String> map = new TreeMap<>();

        for (Map.Entry<String, String> e : enhanced.entrySet()) {
            String origV = orig.get(e.getKey());
            if (!e.getValue().equals(origV))
                map.put(e.getKey(), e.getValue());
        }
        return map;
    }

    private static TreeMap<String, String> readAndDelete(String inPath)
            throws UnsupportedEncodingException, FileNotFoundException, IOException {
        TreeMap<String, String> map = new TreeMap<>();

        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inPath), UTF8))) {
            String line;
            while ((line = in.readLine()) != null) {
                int cut = line.indexOf('=');
                map.put(line.substring(0, cut), line.substring(cut + 1));
            }
        }

        new File(inPath).delete();

        return map;
    }

    private static TreeMap<String, String> getSystemProps() {
        TreeMap<String, String> r = new TreeMap<>();
        for (Object key : System.getProperties().keySet()) {
            String k = key.toString();
            if (k.contains("java.command") || k.contains("class.path"))
                continue;

            r.put(k, System.getProperty(k));
        }
        return r;
    }

    private static TreeMap<String, String> getSystemEnvs() {
        return new TreeMap<>(System.getenv());
    }

    public static class Inner implements IKeep {

        public static void main(String[] args) throws IOException {
            dump(getSystemProps(), args[1], args[0] + ".props");
            dump(getSystemEnvs(), args[1], args[0] + ".envs");
        }

        private static void dump(TreeMap<String, String> map, String filters, String outPath) throws IOException {

            try (PrintWriter out = new PrintWriter(outPath, UTF8)) {

                for (Map.Entry<String, String> e : map.entrySet()) {
                    String k = e.getKey();
                    String v = e.getValue();

                    if (k.contains("=") || containsReturn(k) || containsReturn(v))
                        continue;

                    if (filter(k, filters))
                        continue;

                    out.println(k + "=" + v);
                }
            }
        }

        private static boolean filter(String key, String filters) {
            if (filters == null || filters.equals(NO_PARAMETER))
                return false;

            for (String s : filters.split(" ")) {
                if (key.startsWith(s))
                    return true;
            }
            return false;
        }

        private static boolean containsReturn(String s) {
            s = s.trim();
            if (s.isEmpty())
                return true;
            if (s.contains("\r") || s.contains("\n"))
                return true;
            return false;
        }
    }

}
