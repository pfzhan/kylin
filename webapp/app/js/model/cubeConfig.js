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

KylinApp.constant('cubeConfig', {

  //~ Define metadata & class
  measureParamType: ['column', 'constant'],
  measureExpressions: ['SUM', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT',"TOP_N", 'RAW','EXTENDED_COLUMN'],
  dimensionDataTypes: ["string", "tinyint", "int", "bigint", "date"],
  cubeCapacities: ["SMALL", "MEDIUM", "LARGE"],
  cubePartitionTypes: ['APPEND'],
  joinTypes: [
    {name: 'Left', value: 'left'},
    {name: 'Inner', value: 'inner'}
  ],
  queryPriorities: [
    {name: 'NORMAL', value: 50},
    {name: 'LOW', value: 70},
    {name: 'HIGH', value: 30}
  ],
  measureDataTypes: [
    {name: 'INT', value: 'int'},
    {name: 'BIGINT', value: 'bigint'},
    {name: 'DECIMAL', value: 'decimal'},
    {name: 'DOUBLE', value: 'double'},
    {name: 'DATE', value: 'date'},
    {name: 'STRING', value: 'string'}
  ],
  distinctDataTypes: [
    {name: 'Error Rate < 9.75%', value: 'hllc(10)'},
    {name: 'Error Rate < 4.88%', value: 'hllc(12)'},
    {name: 'Error Rate < 2.44%', value: 'hllc(14)'},
    {name: 'Error Rate < 1.72%', value: 'hllc(15)'},
    {name: 'Error Rate < 1.22%', value: 'hllc(16)'},
    {name: 'Precisely (Only for Integer Family column)', value: 'bitmap'}
  ],
  topNTypes: [
    {name: 'Top 10', value: "topn(10)"},
    {name: 'Top 100', value: "topn(100)"},
    {name: 'Top 1000', value: "topn(1000)"}
  ],
  dftSelections: {
    measureExpression: 'SUM',
    measureParamType: 'column',
    measureDataType: {name: 'BIGINT', value: 'bigint'},
    distinctDataType: {name: 'Error Rate < 4.88%', value: 'hllc12'},
    cubeCapacity: 'MEDIUM',
    queryPriority: {name: 'NORMAL', value: 50},
    cubePartitionType: 'APPEND',
    topN:{name: 'Top 100', value: "topn(100)"}
  },
    dictionaries: ["true", "false"],
  encodings:[
    {name:"dict",value:"value"},
    {name:"fixed_length",value:"fixed_length"},
    {name:"int (deprecated)",value:"int"}
  ],
//    cubes config
  theaditems: [
    {attr: 'name', name: 'Name'},
    {attr: 'detail.model_name', name: 'Model'},
    {attr: 'status', name: 'Status'},
    {attr: 'size_kb', name: 'Cube Size'},
    {attr: 'input_records_count', name: 'Source Records'},
    {attr: 'last_build_time', name: 'Last Build Time'},
    {attr: 'owner', name: 'Owner'},
    {attr: 'create_time_utc', name: 'Create Time'}
  ],
  streamingAutoGenerateMeasure:[
    {name:"year_start",type:"date"},
    {name:"quarter_start",type:"date"},
    {name:"month_start",type:"date"},
    {name:"week_start",type:"date"},
    {name:"day_start",type:"date"},
    {name:"hour_start",type:"timestamp"},
    {name:"minute_start",type:"timestamp"}
  ],
  partitionDateFormatOpt:[
    'yyyy-MM-dd',
    'yyyyMMdd',
    'yyyy-MM-dd HH:mm:ss'
  ],
  partitionTimeFormatOpt:[
    'HH:mm:ss',
    'HH:mm',
    'HH'
  ],
  externalFilterType:[
    'HDFS'
  ],
  rowKeyShardOptions:[
    true,false
  ],
  rawTableIndexOptions:[
    'discrete','fuzzy','sorted'
  ],
  statusNeedNofity:['ERROR', 'DISCARDED', 'SUCCEED'],
  buildDictionaries:[
    {name:"Global Dictionary", value:"org.apache.kylin.dict.GlobalDictionaryBuilder"}
  ]
});
