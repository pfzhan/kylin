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

package io.kyligence.kap.tool.metadata;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * extract cube related info for debugging/distributing purpose
 */
public class MetadataUpgrader {
    private static final Logger logger = LoggerFactory.getLogger(MetadataUpgrader.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            return;
        }
        String metadataDir = args[0];
        upgradeMetadata(new File(metadataDir));
    }

    private static void upgradeMetadata(File dest) throws IOException {
        for (File f : dest.listFiles()) {
            if (f.isDirectory()) {
                upgradeMetadata(f);
            } else {
                upgradeMetadataInternal(f);
            }
        }
    }

    private static void upgradeMetadataInternal(File f) throws IOException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(f);
            boolean replaced = false;

            // Delete signature
            if (rootNode.has("signature")) {
                logger.info("remove signature in file {}", f);
                ((ObjectNode) rootNode).remove("signature");
                replaced = true;
            }

            // Add dimension index="eq"
            if (rootNode.has("rowkey")) {
                JsonNode rowkey_collumns = rootNode.get("rowkey").get("rowkey_columns");
                if (rowkey_collumns != null) {
                    for (int i = 0; i < rowkey_collumns.size(); i++) {
                        JsonNode rowkey = rowkey_collumns.get(i);
                        if (!rowkey.has("index")) {
                            logger.info("add index for rowkey in file {}", f);
                            ((ObjectNode) rowkey).put("index", "eq");
                            replaced = true;
                        }
                    }
                }
            }

            // Upgrade engine and storage type
            if (rootNode.get("engine_type") != null) {
                ((ObjectNode) rootNode).put("engine_type", 100);
                replaced = true;
            }
            if (rootNode.get("storage_type") != null) {
                ((ObjectNode) rootNode).put("storage_type", 100);
                replaced = true;
            }

            if (replaced) {
                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                objectMapper.writeValue(f, rootNode);
            }
        } catch (JsonProcessingException ex) {
            logger.info("cannot parse file {}", f);
        }
    }
}
