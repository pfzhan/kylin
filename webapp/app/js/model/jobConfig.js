/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.constant('jobConfig', {
  dataEnglish:{
  allStatus: [
    {name: 'NEW', value: 0},
    {name: 'PENDING', value: 1},
    {name: 'RUNNING', value: 2},
    {name: 'FINISHED', value: 4},
    {name: 'ERROR', value: 8},
    {name: 'DISCARDED', value: 16}
  ],
  timeFilter: [
    {name: 'LAST ONE DAY', value: 0},
    {name: 'LAST ONE WEEK', value: 1},
    {name: 'LAST ONE MONTH', value: 2},
    {name: 'LAST ONE YEAR', value: 3},
    {name: 'ALL', value: 4},
  ],
  theaditems: [
    {attr: 'name', name: 'Job Name'},
    {attr: 'related_cube', name: 'Cube'},
    {attr: 'progress', name: 'Progress'},
    {attr: 'last_modified', name: 'Last Modified Time'},
    {attr: 'duration', name: 'Duration'}
  ],
  queryitems: [
    {attr: 'server', name: 'Server'},
    {attr: 'sql', name: 'Sql'},
    {attr: 'adj', name: 'Description'},
    {attr: 'running_seconds', name: 'Running Seconds'},
    {attr: 'start_time', name: 'Start Time'},
    {attr: 'last_modified', name: 'Last Modified'},
    {attr: 'thread', name: 'Thread'}
  ]},
  dataChinese:{
    allStatus: [
      {name: '新建', value: 0},
      {name: '等待', value: 1},
      {name: '运行', value: 2},
      {name: '完成', value: 4},
      {name: '错误', value: 8},
      {name: '无效', value: 16}
    ],
    timeFilter: [
      {name: '最近一天', value: 0},
      {name: '最近一周', value: 1},
      {name: '最近一月', value: 2},
      {name: '最近一年', value: 3},
      {name: '所有', value: 4},
    ],
    theaditems: [
      {attr: 'name', name: '任务名称'},
      {attr: 'related_cube', name: 'Cube'},
      {attr: 'progress', name: '进程'},
      {attr: 'last_modified', name: '最后修改时间'},
      {attr: 'duration', name: '耗时'}
    ],
    queryitems: [
      {attr: 'server', name: '服务器'},
      {attr: 'sql', name: 'Sql'},
      {attr: 'adj', name: '描述'},
      {attr: 'running_seconds', name: '运行时间'},
      {attr: 'start_time', name: '开始时间'},
      {attr: 'last_modified', name: '最后修改时间'},
      {attr: 'thread', name: '线程'}
    ]

  }

});
