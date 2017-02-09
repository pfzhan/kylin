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

KylinApp.constant('tableConfig', {
  theaditems: [
    {attr: 'id', name: 'ID'},
    {attr: 'name', name: 'Name'},
    {attr: 'datatype', name: 'Data Type'},
    {attr: 'cardinality', name: 'Cardinality'},
    {attr: 'comment', name: 'Comment'}
  ],
  dataTypes:["tinyint","smallint","int","bigint","float","double","decimal","timestamp","date","string","varchar(256)","char","boolean","binary"],
  columnTypeEncodingMap:{
    "numeric": [
      "dict"
    ],
    "bigint": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "char": [
      "boolean",
      "date",
      "time",
      "dict",
      "fixed_length",
      "fixed_length_hex",
      "integer"
    ],
    "integer": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "int4": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "tinyint": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "double": [
      "dict"
    ],
    "date": [
      "date",
      "time",
      "dict"
    ],
    "float": [
      "dict"
    ],
    "decimal": [
      "dict"
    ],
    "timestamp": [
      "date",
      "time",
      "dict"
    ],
    "real": [
      "dict"
    ],
    "time": [
      "date",
      "time",
      "dict"
    ],
    "long8": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "datetime": [
      "date",
      "time",
      "dict"
    ],
    "smallint": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "varchar": [
      "boolean",
      "date",
      "time",
      "dict",
      "fixed_length",
      "fixed_length_hex",
      "integer"
    ]
  }
});
