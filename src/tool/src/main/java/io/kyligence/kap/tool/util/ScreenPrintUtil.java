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
package io.kyligence.kap.tool.util;

public class ScreenPrintUtil {
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RESET = "\u001B[0m";

    private ScreenPrintUtil() {

    }

    public static void println(String msg) {
        System.out.println(msg);
    }

    private static void printlnColorMsg(String type, String msg) {
        System.out.println(type + msg + ANSI_RESET);
    }

    public static void printlnGreen(String msg) {
        printlnColorMsg(ANSI_GREEN, msg);
    }

    public static void printlnRed(String msg) {
        printlnColorMsg(ANSI_RED, msg);
    }

    public static void printlnYellow(String msg) {
        printlnColorMsg(ANSI_YELLOW, msg);
    }

    public static void printlnBlue(String msg) {
        printlnColorMsg(ANSI_BLUE, msg);
    }

    public static void systemExitWhenMainThread(int code) {
        if (isMainThread()) {
            System.exit(code);
        }
    }

    public static boolean isMainThread() {
        return "main".equalsIgnoreCase(Thread.currentThread().getName());
    }
}
