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

package io.kyligence.kap.tool.bisync.tableau;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

import io.kyligence.kap.tool.bisync.BISyncModel;
import io.kyligence.kap.tool.bisync.tableau.datasource.TableauDatasource;

public class TableauDatasourceModel implements BISyncModel {

    private final TableauDatasource tableauDatasource;

    public TableauDatasourceModel(TableauDatasource tableauDatasource) {
        this.tableauDatasource = tableauDatasource;
    }

    @Override
    public void dump(OutputStream outputStream) {
        try {
            dumpModelAsXML(tableauDatasource, outputStream);
        } catch (XMLStreamException | IOException e) {
            throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e);
        }
    }

    public static void dumpModelAsXML(TableauDatasource BISyncModel, OutputStream outputStream)
            throws XMLStreamException, IOException {
        XmlMapper xmlMapper = new XmlMapper();
        XMLStreamWriter writer = xmlMapper.getFactory().getXMLOutputFactory().createXMLStreamWriter(outputStream);
        xmlMapper.enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
        xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        xmlMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        xmlMapper.getFactory().getXMLOutputFactory().setProperty("javax.xml.stream.isRepairingNamespaces", false);
        xmlMapper.writeValue(writer, BISyncModel);
    }
}
