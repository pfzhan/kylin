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

package io.kyligence.kap.tool.general;

import io.kyligence.kap.common.util.OptionBuilder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import io.kyligence.kap.common.util.EncryptUtil;
import lombok.val;

public class CryptTool extends ExecutableApplication {

    private final Option optionEncryptMethod;

    private final Option optionCharSequence;

    private final Options options;

    public CryptTool() {
        OptionBuilder optionBuilder = OptionBuilder.getInstance();
        optionBuilder.withArgName("ENCRYPT_METHOD");
        optionBuilder.hasArg();
        optionBuilder.withDescription("Specify the encrypt method: [AES, BCrypt]");
        optionBuilder.isRequired();
        optionBuilder.withLongOpt("encrypt-method");
        optionEncryptMethod = optionBuilder.create("e");

        optionBuilder.withArgName("CHAR_SEQUENCE");
        optionBuilder.hasArg();
        optionBuilder.withDescription("Specify the char sequence to be encrypted");
        optionBuilder.isRequired();
        optionBuilder.withLongOpt("char-sequence");
        optionCharSequence = optionBuilder.create("s");

        options = new Options();
        options.addOption(optionEncryptMethod);
        options.addOption(optionCharSequence);
    }

    public static void main(String[] args) {
        val tool = new CryptTool();
        tool.execute(args);
        System.exit(0);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) {
        val encryptMethod = optionsHelper.getOptionValue(optionEncryptMethod);
        val passwordTxt = optionsHelper.getOptionValue(optionCharSequence);

        if ("AES".equalsIgnoreCase(encryptMethod)) {
            // for encrypt password like LDAP password
            System.out.println(EncryptUtil.encrypt(passwordTxt));
        } else if ("BCrypt".equalsIgnoreCase(encryptMethod)) {
            // for encrypt the predefined user password, like ADMIN, MODELER.
            BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder();
            System.out.println(bCryptPasswordEncoder.encode(passwordTxt));
        } else {
            System.out.println("Unsupported encrypt method: " + encryptMethod);
            System.exit(1);
        }
    }
}
