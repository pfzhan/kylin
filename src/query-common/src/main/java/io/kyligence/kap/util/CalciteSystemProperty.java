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
package io.kyligence.kap.util;

/**
 * Calcite introduce CalciteSystemProperty in the new version.
 *
 *  Use this to simplify upgrading
 *
 * @param <T> the type of the property value
 */
public class CalciteSystemProperty<T> {

    /**
     * Whether to run Calcite in debug mode.
     *
     * <p>When debug mode is activated significantly more information is gathered and printed to
     * STDOUT. It is most commonly used to print and identify problems in generated java code. Debug
     * mode is also used to perform more verifications at runtime, which are not performed during
     * normal execution.</p>
     */
    public static final CalciteSystemProperty<Boolean> DEBUG =
            new CalciteSystemProperty<>(Boolean.parseBoolean(System.getProperty("calcite.debug", "FALSE")));


    private final T value;
    private CalciteSystemProperty(T v) {
        this.value = v;
    }
    /**
     * Returns the value of this property.
     *
     * @return the value of this property or <code>null</code> if a default value has not been
     * defined for this property.
     */
    public T value() {
        return value;
    }
}
