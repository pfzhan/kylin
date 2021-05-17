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
package io.kyligence.kap.newten.clickhouse;

import org.testcontainers.containers.FixedHostPortGenericContainer;

public class AzuriteContainer extends FixedHostPortGenericContainer<AzuriteContainer> {
    public static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.9.0";
    public static final String COMMAND = "azurite --skipApiVersionCheck --blobHost 0.0.0.0 -l /data";
    public static final int DEFAULT_BLOB_PORT = 10000;

    private int port;
    private String path;


    public AzuriteContainer(int blobPort, String path) {
        super(AZURITE_IMAGE);
        this.port = blobPort;
        this.path = path;
    }

    @Override
    public void start() {
        this.withFixedExposedPort(port, DEFAULT_BLOB_PORT);
        this.withCommand(COMMAND);
        this.withFileSystemBind(path, "/data");
        super.start();
    }
}
