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

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;

public class HashFunctionTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testChecksum() throws IOException {
        String mainDir = temporaryFolder.getRoot() + "/testChecksum";
        File checkSumFile = new File(mainDir, "a.txt");
        // do not change the data.
        FileUtils.writeStringToFile(checkSumFile, "1111");

        byte[] md5 = HashFunction.MD5.checksum(checkSumFile);
        byte[] sha1 = HashFunction.SHA1.checksum(checkSumFile);
        byte[] sha256 = HashFunction.SHA256.checksum(checkSumFile);
        byte[] sha512 = HashFunction.SHA512.checksum(checkSumFile);

        Assert.assertEquals("B59C67BF196A4758191E42F76670CEBA", DatatypeConverter.printHexBinary(md5));
        Assert.assertEquals("011C945F30CE2CBAFC452F39840F025693339C42", DatatypeConverter.printHexBinary(sha1));
        Assert.assertEquals("0FFE1ABD1A08215353C233D6E009613E95EEC4253832A761AF28FF37AC5A150C",
                DatatypeConverter.printHexBinary(sha256));
        Assert.assertEquals(
                "33275A8AA48EA918BD53A9181AA975F15AB0D0645398F5918A006D08675C1CB27D5C645DBD084EEE56E675E25BA4019F2ECEA37CA9E2995B49FCB12C096A032E",
                DatatypeConverter.printHexBinary(sha512));
    }

}
