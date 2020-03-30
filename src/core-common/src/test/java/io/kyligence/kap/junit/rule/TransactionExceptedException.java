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

package io.kyligence.kap.junit.rule;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import lombok.experimental.Delegate;

public class TransactionExceptedException implements TestRule {
    public static TransactionExceptedException none() {
        return new TransactionExceptedException();
    }

    private TransactionExceptedException() {
        exception = ExpectedException.none();
    }

    public void expectInTransaction(Class<? extends Throwable> type) {
        expect(new ClassTransactionMatcher(type));
    }

    public void expectMessageInTransaction(String message) {
        expect(new MessageTransactionMatcher(message));
    }

    @Delegate
    private ExpectedException exception;

    private static class ClassTransactionMatcher extends BaseMatcher {

        private Class<?> aClass;

        public ClassTransactionMatcher(Class<?> aClass) {
            this.aClass = aClass;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof TransactionException)) {
                return false;
            }
            Throwable t = ((TransactionException) item).getCause();
            return t != null && aClass.isAssignableFrom(t.getClass());
        }

        @Override
        public void describeTo(Description description) {
            // describe nothing
        }
    }

    private static class MessageTransactionMatcher extends BaseMatcher {

        private String message;

        public MessageTransactionMatcher(String message) {
            this.message = message;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof TransactionException)) {
                return false;
            }
            Throwable t = ((TransactionException) item).getCause();
            return t != null && t.getMessage() != null && t.getMessage().contains(message);
        }

        @Override
        public void describeTo(Description description) {
            // describe nothing
        }
    }

}
