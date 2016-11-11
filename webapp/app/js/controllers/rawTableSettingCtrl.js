/**
 * Created by luguosheng on 16/9/26.
 */
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

'use strict';

KylinApp.controller('RawTableSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,RawTablesService,cubesManager,modelsManager,SweetAlert,kylinConfig) {
  $scope.availableColumns = {};
  $scope.selectedColumns = {};
  $scope.wantSetting=false;
  if($scope.state.mode=="view"){
    $scope.wantSetting=true;
  }
  $scope.checkNeedSet=function(){
    $scope.wantSetting=! $scope.wantSetting;
    transferRawTableData();
  }
  $scope.availableTables = [];
  //无后台数据的时候获取基础信息
  var getBaseColumnsData=function(){
    if($scope.getDimColumnsByTable){
      var factTable = $scope.metaModel.model.fact_table;
      var cols = $scope.getDimColumnsByTable(factTable);
      var factSelectAvailable = {};
      for (var i = 0; i < cols.length; i++) {
        cols[i].table = factTable;
        cols[i].isLookup = false;
        factSelectAvailable[cols[i].name] = {selected: false, disabled: false};
      }
      $scope.availableColumns[factTable] = cols;
      $scope.selectedColumns[factTable] = factSelectAvailable;
      $scope.availableTables.push(factTable);
      var lookups = $scope.metaModel.model.lookups;
      for (var j = 0; j < lookups.length; j++) {
        var cols2 = $scope.getDimColumnsByTable(lookups[j].table);
        var lookupSelectAvailable = {};
        for (var k = 0; k < cols2.length; k++) {
          cols2[k].table = lookups[j].table;
          cols2[k].isLookup = true;
          lookupSelectAvailable[cols2[k].name] = {selected: false, disabled: false};
        }
        $scope.availableColumns[lookups[j].table] = cols2;
        $scope.selectedColumns[lookups[j].table] = lookupSelectAvailable;
        if ($scope.availableTables.indexOf(lookups[j].table) == -1) {
          $scope.availableTables.push(lookups[j].table);
        }
      }
      var rawTableColumns = [];
      for (var i in $scope.availableColumns) {
        rawTableColumns = rawTableColumns.concat($scope.availableColumns[i]);
      }
      var measuresColumns=$scope.metaModel.model.metrics;
      for(var i=0;i<measuresColumns.length;i++){
        var measuresColumnsObj={};
        measuresColumnsObj.table=$scope.metaModel.model.fact_table;
        measuresColumnsObj.name=measuresColumns[i];
        rawTableColumns.push(measuresColumnsObj);
      }
      var saveData = {};
      saveData.columns = [];
      var tempObj={};
      for (var i = 0; i < rawTableColumns.length; i++) {
        var cur=rawTableColumns[i];
        if(tempObj[cur.table+cur.name]){
          continue;
        }
        tempObj[cur.table+cur.name]=true;
        var columnObj = {};
        columnObj.index = "discrete";
        columnObj.encoding = "var";
        columnObj.table = cur.table;
        columnObj.column = cur.name;
        if($scope.metaModel.model.partition_desc&&columnObj.table+"."+columnObj.column==$scope.metaModel.model.partition_desc.partition_date_column){
          columnObj.index="sorted";
        }
        saveData.columns.push(columnObj);
      }
      $scope.rawTableColumns = saveData;
      transferRawTableData();
    }
  }
  //检测全局配置是否支持rawtable
  $scope.checkIsSupportRawTable=function(){
    return ($scope.cubeMetaFrame&&$scope.cubeMetaFrame.engine_type&&$scope.cubeMetaFrame.engine_type==100);
  }
  $scope.wantSetting=!($scope.RawTables&&$scope.RawTables.needDelete);
  //加载后台数据
  var loadRawTable = function () {
    RawTablesService.getRawTableInfo({rawTableName: $scope.state.cubeName}, {}, function (request) {
      if(request&&request.columns&&request.columns.length){
        $scope.rawTableColumns = request;
        $scope.hasConfig=true;
        $scope.isSupportRawTable=$scope.checkIsSupportRawTable();
      }else{
        $scope.wantSetting=false;
        getBaseColumnsData();
        if($scope.isEdit){
          $scope.rawTableColumns=$scope.rawTableColumns||{};
          $scope.rawTableColumns.needAdd=true;
        }
        $scope.isSupportRawTable=$scope.checkIsSupportRawTable();
      }
      transferRawTableData();
    },function(){
      $scope.wantSetting=false;
      if($scope.isEdit){
        $scope.rawTableColumns=$scope.rawTableColumns||{};
        $scope.rawTableColumns.needAdd=true;
      }
      $scope.isSupportRawTable=$scope.checkIsSupportRawTable();
    })
  }
  if($scope.RawTables&&$scope.RawTables.columns&&$scope.RawTables.columns.length>=0){
    $scope.rawTableColumns=$scope.RawTables;
    $scope.isSupportRawTable=$scope.checkIsSupportRawTable();
  }else{
    if($scope.state.mode=="view"||$scope.isEdit){
      loadRawTable();
    }else{
      getBaseColumnsData();
      $scope.isSupportRawTable=$scope.checkIsSupportRawTable();
      $scope.wantSetting=false;
    }
  }
  //数据推送给父Controller
  function transferRawTableData(){
    if($scope.wantSetting&&$scope.isSupportRawTable){
      $scope.$emit('RawTableEdited', $scope.rawTableColumns);
    }else{
      var data=[];
      data.needDelete=$scope.hasConfig;
      $scope.$emit('RawTableEdited',data);
    }
  }
  $scope.$watch("rawTableColumns",transferRawTableData,true)
});
