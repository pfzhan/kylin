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

KylinApp.controller('RawTableSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,RawTablesService,cubesManager,modelsManager,SweetAlert,kylinConfig,VdmUtil,TableModel, StringHelper) {
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
  $scope.changeRawTableDataFromServer=function(rawtableData){
    var data=[].concat(rawtableData);
    var len=data&&data.length|| 0;
    for(var i=0;i<len;i++){
      var result = /:(\d+)/.exec(data[i].encoding);
      data[i].valueLength =data[i].valueLength?data[i].valueLength:(result ? result[1] : 0);
      data[i].encoding=$scope.removeVersion(data[i].encoding).replace(/:\d+/,'')+'[v'+(data[i].encoding_version||1)+']';
      data[i].encodingName=$scope.removeVersion(data[i].encoding).replace(/:\d+/,'');
    }
    return data;
  }
  //无后台数据的时候获取基础信息
  var getBaseColumnsData=function(){
    if($scope.getDimColumnsByAlias){
      var factTable = StringHelper.removeNameSpace($scope.metaModel.model.fact_table);
      var cols = $scope.getDimColumnsByAlias(factTable);
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
        var cols2 = $scope.getDimColumnsByAlias(lookups[j].alias);
        var lookupSelectAvailable = {};
        for (var k = 0; k < cols2.length; k++) {
          cols2[k].table = lookups[j].alias;
          cols2[k].isLookup = true;
          lookupSelectAvailable[cols2[k].name] = {selected: false, disabled: false};
        }
        $scope.availableColumns[lookups[j].alias] = cols2;
        $scope.selectedColumns[lookups[j].alias] = lookupSelectAvailable;
        if ($scope.availableTables.indexOf(lookups[j].alias) == -1) {
          $scope.availableTables.push(lookups[j].alias);
        }
      }
      var rawTableColumns = [];
      for (var i in $scope.availableColumns) {
        rawTableColumns = rawTableColumns.concat($scope.availableColumns[i]);
      }
      var measuresColumns=$scope.metaModel.model.metrics;
      for(var i=0;i<measuresColumns.length;i++){
        var measuresColumnsObj={};
        measuresColumnsObj.table=VdmUtil.getNameSpace(measuresColumns[i]);
        measuresColumnsObj.name=VdmUtil.removeNameSpace(measuresColumns[i]);
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
        columnObj.encoding = "orderedbytes[v1]";
        columnObj.table = cur.table;
        columnObj.column = cur.name;
        columnObj.valueLength=0;
        if($scope.metaModel.model.partition_desc&&columnObj.table+"."+columnObj.column==$scope.metaModel.model.partition_desc.partition_date_column){
          columnObj.index="sorted";
        }
        saveData.columns.push(columnObj);
      }
      saveData.columns= $scope.changeRawTableDataFromServer(saveData.columns)
      $scope.rawTableColumns = saveData;
      transferRawTableData();
    }
  }
  //检测全局配置是否支持rawtable
  $scope.checkIsSupportRawTable=function(){
    return ($scope.cubeMetaFrame&&$scope.cubeMetaFrame.engine_type&&($scope.cubeMetaFrame.engine_type==100||$scope.cubeMetaFrame.engine_type==99));
  }
  $scope.wantSetting=!($scope.RawTables&&$scope.RawTables.needDelete);
  $scope.hisRawTableData=[];
  //加载后台数据
  var loadRawTable = function (callback) {
    RawTablesService.getRawTableInfo({rawTableName: $scope.state.cubeName}, {}, function (request) {
      if(request&&request.columns&&request.columns.length){
        request.columns= $scope.changeRawTableDataFromServer(request.columns);
        $scope.rawTableColumns = request;
        $scope.hisRawTableData=request.columns;
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
      if(typeof callback=='function'){
        callback(request.columns)
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
    $scope.rawTableColumns={};
    $scope.rawTableColumns.columns=$scope.changeRawTableDataFromServer($scope.RawTables.columns);
/*    angular.forEach($scope.rawTableColumns.columns,function(column){
      column

    });*/
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


  var encodings =[];
  if($scope.state.mode!="view"){
    encodings =[].concat($scope.store.supportedEncoding);
    encodings.push({
      'name': 'var',
      'value': 'var[v1]',
      'version': 1,
      'baseValue':'var',
      'suggest':true
    })
    encodings.push({
      'name': 'orderedbytes',
      'value': 'orderedbytes[v1]',
      'version': 1,
      'baseValue':'var',
      'suggest':true
    })
  }

  $scope.getRawTableEncodings =function (tableAlias,columnName){
    var filterName=name;
    var columnType=$scope.getColumnTypeByAlias(columnName,tableAlias)||'';
    var filterEncoding;
    encodings=VdmUtil.removeFilterObjectList(encodings,'baseValue','dict');
    var matchList=VdmUtil.getObjValFromLikeKey($scope.store.encodingMaps,columnType);
    matchList.push('var','orderedbytes');
    if($scope.isEdit&&name){
      if($scope.hisRawTableData){
        for(var s=0;s<$scope.hisRawTableData.length;s++){
          if(filterName==$scope.hisRawTableData[s].column){
            var version=$scope.hisRawTableData[s].encoding_version||1;
            filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'value',$scope.hisRawTableData[s].encoding.replace(/:\d+/,"")+(version?"[v"+version+"]":"[v1]"),'suggest',true)
            matchList.push($scope.hisRawTableData[s].encoding.replace(/:\d+/,""))
            filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList);
          }
        }
      }
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
      filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList)
    }else{
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
      filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList)
    }
    return filterEncoding;
  }




});
