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

package io.kyligence.kap.tool.mr;

import java.io.File;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.helpers.DefaultHandler;

public class HadoopConfPropertyRetriever {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.exit(1);
        }

        String confPath = args[0];
        final String targetProperty = args[1];

        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        File file = new File(confPath);
        saxParser.parse(file, new HadoopConfHandler(targetProperty));
    }

    static class HadoopConfHandler extends DefaultHandler {
        private boolean inProperty = false;
        private boolean inName = false;
        private boolean inValue = false;
        private boolean isTargetProperty = false;

        private String propertyName;
        private StringBuilder propertyValue = new StringBuilder();

        public HadoopConfHandler(String propertyName) {
            this.propertyName = propertyName;
        }

        @Override
        public void startElement(String uri, String localName, String qName, org.xml.sax.Attributes attributes)
                throws org.xml.sax.SAXException {
            if (qName.equalsIgnoreCase("property")) {
                inProperty = true;
            } else if (qName.equalsIgnoreCase("name")) {
                inName = true;
            } else if (qName.equalsIgnoreCase("value")) {
                inValue = true;
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws org.xml.sax.SAXException {
            if (inProperty && inName) {
                if (new String(ch, start, length).equalsIgnoreCase(propertyName)) {
                    isTargetProperty = true;
                }
            } else if (inProperty && inValue && isTargetProperty) {
                propertyValue.append(new String(ch, start, length));
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws org.xml.sax.SAXException {
            if (qName.equalsIgnoreCase("property")) {
                if (inProperty && isTargetProperty) {
                    System.out.println(propertyValue.toString());
                    propertyValue.setLength(0);
                }
                inProperty = false;
                isTargetProperty = false;
            } else if (qName.equalsIgnoreCase("name")) {
                inName = false;
            } else if (qName.equalsIgnoreCase("value")) {
                inValue = false;
            }
        }
    }
}
