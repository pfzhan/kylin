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
    {name: 'STOPPED', value: 32},
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
    {attr: 'related_cube', name: 'Table/Model/Cube'},
    {attr: 'progress', name: 'Progress/Status'},
    {attr: 'last_modified', name: 'Last Modified Time'},
    {attr: 'duration', name: 'Duration'},
    {attr: 'actions', name: 'Actions'}
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
  ],
  scheduleritems: [
    // {attr: 'name', name: 'Scheduler Name'},
    {attr: 'relatedCube', name: 'Cube'},
    {attr: 'nextRunTime', name: 'Next Run Time'},
    {attr: 'partitionEnd', name: 'Current Partition End'},
    {attr: 'repeatCount', name: 'Repeat Count'},
    {attr: 'curRepeatCount', name: 'Current Repeat Count'},
    {attr: 'repeatInterval', name: 'Repeat Interval'},
    // {attr: 'partitionInterval', name: 'Partition Interval'},
    {attr: 'actions', name: 'Actions'}
  ]},
  dataChinese:{
    allStatus: [
      {name: '新建', value: 0},
      {name: '等待', value: 1},
      {name: '运行', value: 2},
      {name: '暂停', value: 32},
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
      {attr: 'name', name: '任务'},
      {attr: 'related_cube', name: '表/模型/Cube'},
      {attr: 'progress', name: '进度/状态'},
      {attr: 'last_modified', name: '最后修改时间'},
      {attr: 'duration', name: '耗时'},
      {attr: 'actions', name: '操作'}
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
    ],
    scheduleritems: [
      // {attr: 'name', name: '任务名'},
      {attr: 'relatedCube', name: 'Cube'},
      {attr: 'nextRunTime', name: '下次运行时间'},
      {attr: 'partitionEnd', name: '当前build节点'},
      {attr: 'repeatCount', name: '总build次数'},
      {attr: 'curRepeatCount', name: '已build次数'},
      {attr: 'repeatInterval', name: '重复频率'},
      // {attr: 'partitionInterval', name: '追加build时间'},
      {attr: 'actions', name: '操作'}
    ]
  }

});
