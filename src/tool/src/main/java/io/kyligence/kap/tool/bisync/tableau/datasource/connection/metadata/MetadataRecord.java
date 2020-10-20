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
package io.kyligence.kap.tool.bisync.tableau.datasource.connection.metadata;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class MetadataRecord {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String clazs;

    @JacksonXmlProperty(localName = "remote-name")
    private String remoteName;

    @JacksonXmlProperty(localName = "remote-type")
    private String remoteType;

    @JacksonXmlProperty(localName = "local-name")
    private String localName;

    @JacksonXmlProperty(localName = "parent-name")
    private String parentName;

    @JacksonXmlProperty(localName = "remote-alias")
    private String remoteAlias;

    @JacksonXmlProperty(localName = "ordinal")
    private String ordinal;

    @JacksonXmlProperty(localName = "width")
    private String width;

    @JacksonXmlProperty(localName = "collation")
    private Collation collation;

    @JacksonXmlProperty(localName = "local-type")
    private String localType;

    @JacksonXmlProperty(localName = "padded-semantics")
    private String paddedSemantics;

    @JacksonXmlProperty(localName = "precision")
    private String precision;

    @JacksonXmlProperty(localName = "scale")
    private String scale;

    @JacksonXmlProperty(localName = "aggregation")
    private String aggregation;

    @JacksonXmlProperty(localName = "contains-null")
    private String containsNull;

    @JacksonXmlProperty(localName = "attributes")
    private AttributeList attributeList;

}
