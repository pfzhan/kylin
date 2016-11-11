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
    {attr: 'user', name: 'User'},
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
      {attr: 'user', name: '用户'},
      {attr: 'sql', name: 'Sql'},
      {attr: 'adj', name: '描述'},
      {attr: 'running_seconds', name: '运行时间'},
      {attr: 'start_time', name: '开始时间'},
      {attr: 'last_modified', name: '最后修改时间'},
      {attr: 'thread', name: '线程'}
    ]

  }

});
