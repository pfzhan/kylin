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

KylinApp.controller('streamingConfigCtrl', function ($scope,StreamingService, $q, $routeParams, $location, $window, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, modelConfig, ProjectModel, ModelService, MetaModel, modelsManager, cubesManager, TableModel, $animate,StreamingModel,kylinCommon,ClusterService,VdmUtil,cubeConfig,tableConfig) {

  $scope.tableModel = TableModel;
  $scope.cubeConfig = cubeConfig;
  $scope.tableConfig = tableConfig;
  $scope.selectedSrcDb = [];
  if($scope.state.mode=='view') {
    $scope.streamingMeta = StreamingModel.createStreamingConfig();
    $scope.kafkaMeta = StreamingModel.createKafkaConfig();
  }
  $scope.filterColumnType=function(typeName){
    if(typeName&&typeName.indexOf('decimal')>=0){
      return typeName.replace(/\(.*?\)/,'');
    }
    return typeName;
  }
  $scope.tableColumnTrans=function(tableColumn){
    var resultArr=[];
    for(var i= 0,len=tableColumn.length||0;i<len;i++){
      var obj={}
      obj.name=tableColumn[i].name;
      obj.checked='Y';
      obj.type=$scope.filterColumnType(tableColumn[i].datatype);
      if($scope.cubeConfig.streamingAutoGenerateMeasure.some(function(list){return obj.name.toUpperCase()==list.name.toUpperCase()})){
        obj.fromSource='N';
      }else{
        obj.fromSource='Y';
      }
      resultArr.push(obj);
    }
    return resultArr;
  }


  if($scope.state.mode=='edit'&& $scope.state.target=='kfkConfig' && $scope.state.tableName){
    StreamingService.getConfig({table:$scope.state.tableName}, function (configs) {
      if(!!configs[0]&&configs[0].name.toUpperCase() == $scope.state.tableName.toUpperCase()){
        $scope.updateStreamingMeta(configs[0]);
        StreamingService.getKfkConfig({kafkaConfigName:$scope.streamingMeta.name}, function (streamings) {
          if(!!streamings[0]&&streamings[0].name.toUpperCase() == $scope.state.tableName.toUpperCase()){
            $scope.updateKafkaMeta(streamings[0]);
            $scope.loadClusterInfo();
          }
        })
      }
    })
    $scope.table.columnList=$scope.tableColumnTrans($scope.tableModel.selectedSrcTable.columns);
  }

  //过滤column配置
  $scope.loadColumnZH=function(){
    if($scope.state.target){
      return;
    }
    $scope.streamingCfg.columnOptions = [];
    $scope.rule.timestampColumnExist = false;
    angular.forEach($scope.table.columnList, function (column, $index) {
      if (column.checked == "Y" && column.fromSource == "Y" && column.type == "timestamp") {
        $scope.streamingCfg.columnOptions.push(column.name);
        $scope.rule.timestampColumnExist = true;
      }
    })
    if ($scope.streamingCfg.columnOptions.length >= 1) {
      $scope.streamingCfg.parseTsColumn = $scope.streamingCfg.columnOptions[0];
      $scope.kafkaMeta.parserProperties = "tsColName=" + $scope.streamingCfg.parseTsColumn;
    }else{
      $scope.streamingCfg.parseTsColumn =[];
      $scope.kafkaMeta.parserProperties ="";
    }
  }




  //通过json来解析columnList
  $scope.convertJson = function (jsonData) {
    //$scope.table.schemaChecked = true;
    try {
      var parseResult =JSON.parse(jsonData);
    } catch (error) {
      //$scope.table.sourceValid = false;
      return;
    }
    //$scope.table.sourceValid = true;
    //streaming table data change structure
    var columnList = []

    function changeObjTree(obj, base) {
      base = base ? base + "_" : "";
      for (var i in obj) {
        if (Object.prototype.toString.call(obj[i]) == "[object Object]") {
          changeObjTree(obj[i], base + i);
          continue;
        }
        columnList.push(createNewObj(base + i, obj[i]));
      }
    }

    function checkValType(val, key) {
      var defaultType;
      if (typeof val === "number") {
        if (/id/i.test(key) && val.toString().indexOf(".") == -1) {
          defaultType = "int";
        } else if (val <= 2147483647) {
          if (val.toString().indexOf(".") != -1) {
            defaultType = "decimal";
          } else {
            defaultType = "int";
          }
        } else {
          defaultType = "timestamp";
        }
      } else if (typeof val === "string") {
        if (!isNaN((new Date(val)).getFullYear()) && typeof ((new Date(val)).getFullYear()) === "number") {
          defaultType = "date";
        } else {
          defaultType = "varchar(256)";
        }
      } else if (Object.prototype.toString.call(val) == "[object Array]") {
        defaultType = "varchar(256)";
      } else if (typeof val === "boolean") {
        defaultType = "boolean";
      }
      return defaultType;
    }

    function createNewObj(key, val) {
      var obj = {};
      obj.name = key;
      obj.type = checkValType(val, key);
      obj.value=val;
      obj.fromSource = "Y";
      obj.checked = "Y";
      if (Object.prototype.toString.call(val) == "[object Array]") {
        obj.checked = "N";
      }
      return obj;
    }

    changeObjTree(parseResult);

    var timeMeasure = $scope.cubeConfig.streamingAutoGenerateMeasure;
    for (var i = 0; i < timeMeasure.length; i++) {
      var defaultCheck = 'Y';
      columnList.push({
        'name': timeMeasure[i].name,
        'checked': defaultCheck,
        'type': timeMeasure[i].type,
        'value':null,
        'fromSource': 'N'
      });
    }
    return columnList;
  }

  $scope.streamingOnChange=function(){
    $scope.table.columnList=$scope.convertJson($scope.streaming.sourceSchema);
  }

  $scope.convertSampleData=function(arr){
    var result=[];
    for(var i=0;i<(arr&&arr.length||0);i++){
      var obj={};
      var baseObj=$scope.convertJson(arr[i]);
      for(var m=0;m<baseObj.length||0;m++){
        obj[baseObj[m].name]=baseObj[m].value
      }
      result.push(angular.toJson(obj));
    }
    return result;
  }
  $scope.addCluster = function () {
    $scope.kafkaMeta.clusters.push(StreamingModel.createKafkaCluster());
  };



  $scope.removeCluster = function(cluster){

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_remove_cluster,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm) {
        var index = $scope.kafkaMeta.clusters.indexOf(cluster);
        if (index > -1) {
          $scope.kafkaMeta.clusters.splice(index, 1);
        }
        $scope.loadClusterInfo();
      }

    })
  }
  $scope.currentCheck=-1;
  $scope.addTag=true;
  $scope.addBroker = function (cluster,broker,index) {

    $scope.currentCheck=index;
    $scope.fromError=false;
    //$scope.modelsManager.selectedModel = model;
    cluster.newBroker=(!!broker)?broker:StreamingModel.createBrokerConfig();
    $scope.addTag=!broker;
  };
  if($scope.state.mode=='edit'&&!$scope.state.target){
    $scope.addCluster();
    $scope.addBroker($scope.kafkaMeta.clusters[0])
  }
  $scope.removeNewBroker = function (cluster){
    $scope.fromError=false;
    delete cluster.newBroker;
  }

  $scope.removeElement = function (cluster, element) {
    var index = cluster.brokers.indexOf(element);
    if (index > -1) {
      cluster.brokers.splice(index, 1);
    }
  };

  $scope.saveNewBroker = function(cluster){
    $scope.fromError=false;
    var checkResult=false;
    cluster.brokers.forEach(function(item,index){
      if(index!=$scope.currentCheck){
        if(item.port==cluster.newBroker.port&&item.host==cluster.newBroker.host||item.id==cluster.newBroker.id){
          checkResult=true;
        }
      }
    })
    if(checkResult||(cluster.newBroker.port==''||cluster.newBroker.host==''||cluster.newBroker.id=='')){
      $scope.fromError=true;
      return;
    }else{
      if($scope.currentCheck>=0){
        cluster.brokers[$scope.currentCheck]=cluster.newBroker;
      }else{
        cluster.brokers.push(cluster.newBroker);
      }
    }
    $scope.addTag=false;
    $scope.currentCheck=-1;
    delete cluster.newBroker;

  }

  $scope.clearNewBroker = function(cluster){
    $scope.fromError=false;
    $scope.addTag=false;
    $scope.currentCheck=-1;
    delete cluster.newBroker;
  }


  $scope.streamingTsColUpdate = function(){
    if(!$scope.streamingCfg.parseTsColumn){
      $scope.streamingCfg.parseTsColumn = ' ';
    }
    $scope.kafkaMeta.parserProperties = "tsColName="+$scope.streamingCfg.parseTsColumn;
  }

  $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    //view model
    if($scope.state.mode == 'view' && $scope.tableModel.selectedSrcTable.source_type==1){
      $scope.reloadMeta();
    }

  });


  $scope.$on('StreamingConfigEdited', function (event) {
    $scope.reloadMeta();
  });

  $scope.reloadMeta = function(){
    var table = $scope.tableModel.selectedSrcTable;
    var streamingName = table.database+"."+table.name;
    $scope.streamingMeta = {};
    $scope.kafkaMeta = {};
    StreamingService.getConfig({table:streamingName}, function (configs) {
      if(!!configs[0]&&configs[0].name.toUpperCase() == streamingName.toUpperCase()){
        $scope.streamingMeta = configs[0];
        StreamingService.getKfkConfig({kafkaConfigName:$scope.streamingMeta.name}, function (streamings) {
          if(!!streamings[0]&&streamings[0].name.toUpperCase() == streamingName.toUpperCase()){
            $scope.kafkaMeta = streamings[0];
          }
        })
      }
    })
  }

  $scope.loadTopicSampleData=function(cluster,topic,callback,errorcallback){
    loadingRequest.show();
    ClusterService.getTopicInfo({
      cluster:cluster,
      topic:topic,
    },{
      project: $scope.projectName,
      tableData: angular.toJson($scope.tableData),
      streamingConfig: angular.toJson($scope.streamingMeta),
      kafkaConfig: angular.toJson($scope.kafkaMeta)
    },function(data){
      loadingRequest.hide();
      if(typeof callback=='function'){
        callback(data);
      }
    },function(e){
      var message;
      if (e.data && e.data.exception) {
        message = e.data.exception;
      } else {
        message = $scope.dataKylin.alert.error_info;
      }
      loadingRequest.hide();
      if(typeof errorcallback=='function'){
        errorcallback(message);
      }
    })
  }


  $scope.reloadTopicSampleData=function(){
    $scope.loadTopicSampleData(kafkaMeta. kafkaMeta.topic)
  }
  $scope.treeData=[];
  $scope.loadClusterInfo=function(){
    if($scope.state.target){
      return;
    }
    $scope.treeData=[];
    $scope.loading = true;
    var hasHisTopic=false;
    ClusterService.getCusterTopic({},{
      project: $scope.projectName,
      tableData: angular.toJson($scope.tableData),
      streamingConfig: angular.toJson($scope.streamingMeta),
      kafkaConfig: angular.toJson($scope.kafkaMeta)
    },function(data){
      $scope.loading = false;
      for(var i in data){
        if(VdmUtil.isNotExtraKey(data,i)){
          var obj={}
          obj.label=$scope.dataKylin.cube.kaCluster+'-1('+i.replace(/-/g,'.')+')';
          obj.data=i;
          obj.children=[];
          for(var k=0;k<data[i].length;k++){
            var childObj={};
            childObj.label=data[i][k];
            childObj.data=data[i][k];
            childObj.pdata=i;
            childObj.icon='indented tree-icon fa fa-table';

            if($scope.kafkaMeta.topic&&$scope.kafkaMeta.topic==data[i][k]){
              hasHisTopic=true;
            }
            childObj.onSelect=function(branch) {
              loadingRequest.show();
              $scope.kafkaMeta.topic=branch.data;
              $scope.loadTopicSampleData(branch.pdata,branch.data,function(data){
                if(!data||data&&data.length==0){
                  SweetAlert.swal('', $scope.dataKylin.data_source.useLessTopic, 'error');
                }else{
                  $scope.streaming.sourceSchema= data[0]||'';
                  $scope.table.message=$scope.convertSampleData(data);
                }
              },function(){
                SweetAlert.swal('', message, 'error');
              })
            }
            obj.children.push(childObj);
          }
          $scope.treeData.push(obj);
        }
        //当前列表中没有了之前选择的topic，就清空之前根据topic设置的内容
        if(!hasHisTopic){
          $scope.streaming.sourceSchema='';
          $scope.table.message=[];
          $scope.streamingOnChange();
          $scope.loadColumnZH();
        }
      }
    },function(){
      $scope.loading = false;
      $scope.streaming.sourceSchema='';
      $scope.table.message=[];
      $scope.streamingOnChange();
      $scope.loadColumnZH();
    })
  }

  $scope.setSampleData=function(table,messages,callback,errcallback){
    ClusterService.saveSampleData({
      table:table,
      action:'samples'
    },messages,function(data){
      if(data&&data[0]+data[1].toUpperCase()=='OK'){
        callback();
      }else{
        errcallback();
      }
    },function(){
      errcallback();
    })
  }
  $scope.$on('reloadSampleData', function(d,tableName) {
    loadingRequest.show();
    ClusterService.getSampleData({
      table:tableName,
      action:'update_samples'
    },{},function(data){
        if(!data||data&&data.length<=0){
          SweetAlert.swal('', $scope.dataKylin.alert.getNoMessages, 'error');
          loadingRequest.hide();
          return;
        }
       $scope.message=$scope.convertSampleData(data);
        $scope.setSampleData(tableName,$scope.message,function(){
          SweetAlert.swal('', $scope.dataKylin.alert.streamingSaveMsg, 'success');
          $scope.getSampleData();
          loadingRequest.hide();
        },function(){
          SweetAlert.swal('', $scope.dataKylin.alert.streamingSaveMsgError, 'error');
          loadingRequest.hide();
        });
    },function(){
      loadingRequest.hide();
    })

  });
  $scope.dataBaseList=[]
  TableModel.initTableData(function(data){
    $scope.dataBaseList=TableModel.getDataBaseList();
  })



});
