/**
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

package io.kyligence.kap.cube.index.pinot.util;

import java.util.Comparator;

public class Pairs {

    public static IntPair intPair(int a, int b) {
        return new IntPair(a, b);
    }

    public static Comparator<IntPair> intPairComparator() {
        return new AscendingIntPairComparator();
    }

    public static class IntPair {
        int a;

        int b;

        public IntPair(int a, int b) {
            this.a = a;
            this.b = b;
        }

        public int getLeft() {
            return a;
        }

        public int getRight() {
            return b;
        }

        public void setLeft(int a) {
            this.a = a;
        }

        public void setRight(int b) {
            this.b = b;
        }

        @Override
        public String toString() {
            return "[" + a + "," + b + "]";
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof IntPair) {
                IntPair that = (IntPair) obj;
                return obj != null && a == (that.a) && b == that.b;
            }
            return false;
        }
    }

    public static class AscendingIntPairComparator implements Comparator<IntPair> {

        @Override
        public int compare(IntPair o1, IntPair o2) {
            return Integer.compare(o1.a, o2.a);
        }
    }

    public static Comparator<Number2ObjectPair> getAscendingnumber2ObjectPairComparator() {
        return new AscendingNumber2ObjectPairComparator();
    }

    public static Comparator<Number2ObjectPair> getDescendingnumber2ObjectPairComparator() {
        return new DescendingNumber2ObjectPairComparator();
    }

    public static class Number2ObjectPair<T> {
        Number a;

        T b;

        public Number2ObjectPair(Number a, T b) {
            this.a = a;
            this.b = b;
        }

        public Number getA() {
            return a;
        }

        public T getB() {
            return b;
        }
    }

    public static class AscendingNumber2ObjectPairComparator implements Comparator<Number2ObjectPair> {
        @Override
        public int compare(Number2ObjectPair o1, Number2ObjectPair o2) {
            return new Double(o1.a.doubleValue()).compareTo(new Double(o2.a.doubleValue()));
        }
    }

    public static class DescendingNumber2ObjectPairComparator implements Comparator<Number2ObjectPair> {
        @Override
        public int compare(Number2ObjectPair o1, Number2ObjectPair o2) {
            return new Double(o2.a.doubleValue()).compareTo(new Double(o1.a.doubleValue()));
        }
    }
}
