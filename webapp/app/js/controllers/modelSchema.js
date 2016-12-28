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

KylinApp.controller('ModelSchemaCtrl', function ($scope,$timeout, $routeParams,VdmUtil,QueryService, UserService,TableService, ProjectService, $q, AuthenticationService, $filter, ModelService, MetaModel, CubeDescModel, CubeList, TableModel, ProjectModel, $log, SweetAlert, modelsManager,language, modelsEdit,DrawHelper,$modal,cubeConfig,loadingRequest,$location,ModelDescService,CubeService,TableExtService,StorageHelper) {
  $scope.modelsManager = modelsManager;
  $scope.projectModel = ProjectModel;
  $scope.projects = [];
  $scope.newDimension = null;
  $scope.newMeasure = null;
  $scope.forms = {};
  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if(!$scope.projectModel.getSelectedProject()) {
      return;
    }else{
      $scope.state.project=newValue;

      if ($scope.state.mode == "edit") {
        var defer = $q.defer();
        var queryParam = {};
        if (!$scope.projectModel.isSelectedProjectValid()) {
          return;
        }
        queryParam.projectName = $scope.projectModel.selectedProject;
        modelsManager.list(queryParam).then(function (resp) {
          defer.resolve(resp);
          modelsManager.loading = false;
          return defer.promise;
        });
      }
      var param = {
        ext: true,
        project:newValue
      };
      if(newValue){
        TableModel.initTables();
        TableService.list(param, function (tables) {
          angular.forEach(tables, function (table) {
            table.name = table.database+"."+table.name;
            TableModel.addTable(table);
          });
        });
      }
    }
  })

  $scope.dataKylin =  language.getDataKylin();
  $scope.$on("finish",function (event,data) {
    $scope.dataKylin =  language.getDataKylin();
    for(var i in $scope.wizardSteps){
      $scope.wizardSteps[i].title = $scope.dataKylin.model.schema[i];
    }
  });






  //snow
//snow part
  jsPlumb.ready(function() {
    console.log('draw init start');
    //画线对象===================================
    $timeout(function(){
      var treeDom=$('ul.abn-tree');
      treeDom.sortable({
        helper:'clone',
        connectWith:'#modelDesign',
        delay: 150,
        items: '.level-1,.level-2',
        stop:function(event,ui){
          treeDom.sortable( 'cancel' );
        }}).disableSelection();


      $("#modelDesign").sortable({
        receive:function(event,ui){
          var pos=[0,0];
          var domOffset= $("#modelDesign").offset();
          if($("#modelDesign").offset()){
            pos[0]=event.clientX-domOffset.left;
            pos[1]=event.clientY-domOffset.top;
          }
          treeDom.sortable('cancel');
          var database=$(ui.item).attr("data");

          TableModel.initTableData(function(){
            var tableDataList=TableModel.getTableDatail(database);

            if(tableDataList&&tableDataList.length<2){
              var table=tableDataList[0];
              if(DrawHelper.checkHasThisAlias(table.name)){
                table.alias=table.name+'_'+DrawHelper.tableList.data.length
              }
              ModelService.suggestion({
                table:table.database+'.'+table.name
              },function(data){
                for(var i=0;i<table.columns.length;i++){
                  for(var m in data){
                    if(m=table.columns[i].name){
                      table.columns[i].kind=data[m].toLowerCase().split('_')[0];
                    }else{
                      table.columns[i].kind='disable';
                    }
                  }
                }
                DrawHelper.addTable(table,pos);

              },function(){
                DrawHelper.addTable(table,pos);
              })
            }
          })
        }
      })
      //删除table弹窗
      function openDelDialog(table,callback){
        var delTableCtrl=function($scope, $modalInstance,table,scope){
          $scope.table=table;
          $scope.dataKylin=scope.dataKylin;
          console.log($scope.dataKylin)
          $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
          }
          //移除table
          $scope.remove=function(){
            if(DrawHelper.getTableLinksCount(table.guid)>0){
              $scope.hasUnRemoveLink=true;
            }else{
              DrawHelper.removeTable(table.guid);
              $modalInstance.dismiss('cancel');
            }

          }
        }
        var modalInstance = $modal.open({
          windowClass:'modal_snowtableInfo',
          templateUrl: 'partials/models/snowmodel_info.html',
          controller: delTableCtrl,
          backdrop: 'static',
          resolve:{
            table:function() {
              return table;
            },scope:function(){
              return $scope;
            }
          }
        });
      }
      //缓存提示弹窗
      function cacheTipDialog(callback){
        var cacheTipCtrl=function($scope, $modalInstance,scope){
          $scope.dataKylin=scope.dataKylin;
          console.log($scope.dataKylin)
          $scope.cancel = function () {
            DrawHelper.removeCache();
            $modalInstance.dismiss('cancel');
            callback()
          }
          //移除table
          $scope.ok=function(){
            $modalInstance.dismiss('cancel');
            callback();
          }
        }
        var modalInstance = $modal.open({
          windowClass:'modal_snowtableInfo',
          templateUrl: 'partials/models/snowmodel_cachetip.html',
          controller: cacheTipCtrl,
          backdrop: 'static',
          resolve:{
            scope:function(){
              return $scope;
            }
          }
        });
      }
      //选择join类型弹窗
      function openSelectJoinTypeDialog(conn){
        var snowTableJoinTypeCtrl=function($scope,$modalInstance,conn,scope){
          var linkData=DrawHelper.connects[conn.id];
          $scope.dataKylin=scope.dataKylin;
          if(linkData){
            $scope.link={};
            $scope.link.type=linkData[2]||'inner';
          }
          $scope.conn=conn;
          $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
          }
          $scope.remove=function(){
            DrawHelper.instance.detach(conn);
            delete DrawHelper.connects[conn.id];
            $modalInstance.dismiss('cancel');
          }
          $scope.editJoinType=function(){
            conn.getOverlay("label").setLabel($scope.link.type);
            DrawHelper.updataConnectsType(DrawHelper.connects[conn.id][0].split('.')[0],DrawHelper.connects[conn.id][1].split('.')[0],$scope.link.type)
            $modalInstance.dismiss('cancel');
          }
        }
        var modalInstance = $modal.open({
          windowClass:'modal_jointype',
          templateUrl: 'partials/models/snowmodel_jointype.html',
          controller: snowTableJoinTypeCtrl,
          backdrop: 'static',
          resolve:{
            conn:function(){
              return conn;
            },scope:function(){
              return $scope;
            }
          }
        });
      }


      //选择date类型弹窗
      function openDateFormatTypeDialog(callback,whatDate,hisObj){
        var dateFormatTypeCtrl=function($scope,$modalInstance,callback,cubeConfig,whatDate,scope){
          $scope.callback=callback;
          $scope.dateTypeSet={dateType:''};
          if(whatDate.type=='date'){
            $scope.dateTypeSet={dateType:'yyyy-MM-dd'};
          }else{
            $scope.dateTypeSet={dateType:'HH:mm:ss'};
          }
          if(hisObj){
            $.extend($scope.dateTypeSet,hisObj);
          }
          $scope.dataKylin=scope.dataKylin;
          if(whatDate.type=='date'&&!DrawHelper.isNullObj(DrawHelper.partitionDate)||whatDate.type=='time'&&!DrawHelper.isNullObj(DrawHelper.partitionTime)){
            $scope.showRemoveBtn=true;
          }
          $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
          }
          $scope.remove=function(){
               $scope.callback('del');
               $modalInstance.dismiss('cancel');
          }
          $scope.editDateFormatType=function(){
            if(whatDate.type=='date'){
              $scope.callback($scope.dateTypeSet.dateType);
            }else{
              $scope.callback($scope.dateTypeSet.dateType);
            }
            $modalInstance.dismiss('cancel');
          }
          $scope.cubeConfig=cubeConfig;
          $scope.whatDate=whatDate;
        }
        var modalInstance = $modal.open({
          windowClass:'modal_dateformat',
          templateUrl: 'partials/models/snowmodel_dateformat.html',
          controller: dateFormatTypeCtrl,
          backdrop: 'static',
          resolve:{
            callback:function(){
              return callback;
            },
            cubeConfig:function(){
              return cubeConfig;
            },
            whatDate:function(){
              return whatDate;
            },
            scope:function(){
              return $scope;
            }
          }
        });
      }
      //弹出json内容
      function openJsonDialog(){
        var snowJsonCtr=function($scope,$modalInstance,scope){
            $scope.dataKylin=scope.dataKylin;
            $scope.snowModelJson=angular.toJson(DrawHelper.plumbDataToKylinData(),true);
            $scope.cancel = function () {
              $modalInstance.dismiss('cancel');
            }
        }
        var modalInstance = $modal.open({
          windowClass:'modal_snowmodeljson',
          templateUrl: 'partials/models/snowmodel_json.html',
          controller: snowJsonCtr,
          backdrop: 'static',
          resolve:{
            scope:function(){
              return $scope;
            }
          }
        });
      }

      //model 保存弹窗
      function openModelSaveDialog(conn){
        var snowModelSaveCtrl=function($scope,$modalInstance,SweetAlert,VdmUtil,scope,MessageService,loadingRequest,$location,modelsManager){
         $scope.dataKylin=scope.dataKylin;
         $scope.cubeLen=scope.cubeLengthOfModel;
         if(DrawHelper.kylinData){
           $scope.model={uuid:DrawHelper.kylinData.uuid,name:DrawHelper.kylinData.name, description:DrawHelper.kylinData.description, filter:DrawHelper.kylinData.filter_condition};
         }else{
           $scope.model={
             name:'',
             description:'',
             filter:''
           }
         }
         $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
         }
         $scope.checkModelName = function (modelName) {
            var models = modelsManager.modelNameList;
            if(models.indexOf(modelName) != -1 || models.indexOf(modelName.toLowerCase()) !=-1){
              return false;
            }
            return true;
          }

          $scope.saveModel=function(){
            DrawHelper.instanceName=$scope.model.name;
            DrawHelper.instanceDiscribe=$scope.model.description;
            DrawHelper.filterStr=$scope.model.filter;
            $scope.validate={};



            var saveData;
            try {
              saveData=angular.fromJson(DrawHelper.plumbDataToKylinData());
            } catch (e) {
              SweetAlert.swal(scope.dataKylin.alert.oops, scope.dataKylin.alert.tip_invalid_model_json, 'error');
              return;
            }
              if(DrawHelper.kylinData&&DrawHelper.kylinData.uuid){

                var updateModelData= $.extend({},DrawHelper.kylinData,saveData);
                ModelService.update({}, {
                  modelDescData:VdmUtil.filterNullValInObj(updateModelData),
                  modelName: saveData.name,
                  project:scope.state.project
                }, function (request) {
                  if (request.successful) {
                    //$scope.state.modelSchema = request.modelSchema;
                    DrawHelper.removeCache();
                    $modalInstance.dismiss('cancel');
                    SweetAlert.swal('', scope.dataKylin.alert.success_updated_model, 'success');
                    $location.path("/models");
                    //location.reload();
                  } else {
                    //$scope.saveModelRollBack();
                    var message =request.message;
                    var msg = !!(message) ? message : scope.dataKylin.alert.error_info;
                    MessageService.sendMsg(scope.modelResultTmpl({'text':msg,'schema':''}), 'error', {}, true, 'top_center');
                  }
                  //end loading
                  loadingRequest.hide();
                }, function (e) {
                  //$scope.saveModelRollBack();

                  if(e.data&& e.data.exception){
                    var message =e.data.exception;
                    var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;

                    MessageService.sendMsg(scope.modelResultTmpl({'text':msg,'schema':''}), 'error', {}, true, 'top_center');
                  } else {
                    MessageService.sendMsg(scope.modelResultTmpl({'text':scope.dataKylin.alert.error_info,'schema':''}), 'error', {}, true, 'top_center');
                  }
                  loadingRequest.hide();
                });
              }else{
                if(!$scope.checkModelName($scope.model.name)){
                  $scope.validate.errorRepeatName=true;
                  return;
                }
                ModelService.save({}, {
                  modelDescData:VdmUtil.filterNullValInObj(saveData),
                  project: scope.state.project
                }, function (request) {
                  if(request.successful) {
                    DrawHelper.removeCache();
                    SweetAlert.swal('', scope.dataKylin.alert.success_created_model, 'success');
                    $modalInstance.dismiss('cancel');
                    $location.path("/models");
                    //location.reload();
                  } else {
                    var message =request.message;
                    var msg = !!(message) ? message : scope.dataKylin.alert.error_info;
                    MessageService.sendMsg(scope.modelResultTmpl({'text':msg,'schema':''}), 'error', {}, true, 'top_center');
                  }

                  //end loading
                  loadingRequest.hide();
                }, function (e) {
                  if (e.data && e.data.exception) {
                    var message =e.data.exception;
                    var msg = !!(message) ? message : scope.dataKylin.alert.error_info;
                    MessageService.sendMsg(scope.modelResultTmpl({'text':msg,'schema':scope.state.modelSchema}), 'error', {}, true, 'top_center');
                  } else {
                    MessageService.sendMsg(scope.modelResultTmpl({'text':scope.dataKylin.alert.error_info,'schema':''}), 'error', {}, true, 'top_center');
                  }
                  //end loading
                  loadingRequest.hide();
                });
              }
          }
        }
        DrawHelper.checkSaveData(function(errcode){
          DrawHelper.showTips(errcode);
        },function(){
          var modalInstance = $modal.open({
            windowClass:'modal_modelsave',
            templateUrl: 'partials/models/snowmodel_saveinfo.html',
            controller: snowModelSaveCtrl,
            backdrop: 'static',
            resolve:{
              scope: function () {
                return $scope;
              }
            }
          });
        })
      }

      //拖拽建模（雪花）初始化入口
      DrawHelper=DrawHelper.initService();
      function loadDataRender(initPara,cache){
        DrawHelper.init(initPara);
        if(cache){
          DrawHelper.restoreCache();
        }
        DrawHelper.container.on('click','p',function(e){
          var columnName=$(this).attr('data');
          var tableName=$(this).parent().attr('data');
          $scope.showColumnInfo(tableName,columnName);
          var showInfoPanel=DrawHelper.container.parent().find('.tableColumnInfoBox');
          if(!showInfoPanel.is(':visible')){
            showInfoPanel.fadeIn();
          }
          e.stopPropagation();
        })
      }
      //编辑和试图模式
      if ($scope.isEdit = !!$routeParams.modelName||$scope.state.mode=='view') {
        if($scope.state.mode!='view'){
          $scope.cubeLengthOfModel=0;
          CubeService.list({modelName:$routeParams.modelName}, function (_cubes) {
            $scope.cubeLengthOfModel=_cubes.length;
          })

        }
        var modelName = $routeParams.modelName||$scope.state.modelName;

        ModelDescService.query({model_name: modelName}, function (model) {
          if (model) {
            $scope.modelCopy = angular.copy(model);
            modelsManager.setSelectedModel($scope.modelCopy);
            modelsEdit.setSelectedModel(model);
            $scope.lookupLength=modelsEdit.selectedModel.lookups.length;

            DrawHelper.init({
              delTable:openDelDialog,
              changeConnectType:openSelectJoinTypeDialog,
              addColumnToPartitionDate:openDateFormatTypeDialog,
              addColumnToPartitionTime:openDateFormatTypeDialog,
              saveModel:openModelSaveDialog,
              kylinData:$scope.modelCopy,
              useCache:false,
              openJsonDialog:openJsonDialog,
              actionLockForView:$scope.state.mode=='view',
              actionLockForEdit:$scope.isEdit,
              usedColumnsMap:$scope.selectedColumns
            });
            DrawHelper.kylinDataToJsPlumbData();
            var param = {
              ext: true,
              project:DrawHelper.projectName
            };
            TableModel.initTables();
            TableService.list(param, function (tables) {
              angular.forEach(tables, function (table) {
                table.name = table.database+"."+table.name;
                TableModel.addTable(table);
              });
            });
            DrawHelper.container.on('click','p',function(e){
              var columnName=$(this).attr('data');
              var tableName=$(this).parent().attr('data');
              $scope.showColumnInfo(tableName,columnName);
              var showInfoPanel=DrawHelper.container.parent().find('.tableColumnInfoBox');
              if(!showInfoPanel.is(':visible')){
                showInfoPanel.fadeIn();
              }
              e.stopPropagation();
            })
          }
        });
      }else{
        //添加模式
        var initPara={
          kylinData:angular.fromJson(StorageHelper.storage.get('snowModelJsDragData')),
          delTable:openDelDialog,
          changeConnectType:openSelectJoinTypeDialog,
          addColumnToPartitionDate:openDateFormatTypeDialog,
          addColumnToPartitionTime:openDateFormatTypeDialog,
          saveModel:openModelSaveDialog,
          openJsonDialog:openJsonDialog
        }
        //载入缓存模式
        if(StorageHelper.storage.get('snowModelJsDragData')){
          cacheTipDialog(function(){
            if(StorageHelper.storage.get('snowModelJsDragData')){
              var storeData=angular.fromJson(StorageHelper.storage.get('snowModelJsDragData'));
              initPara.tableList=DrawHelper.tableList;
              initPara.tableList.data=storeData.tableList.data;
              initPara.connects=storeData.connects;
              initPara.partitionDate=storeData.partitionDate;
              initPara.partitionTime=storeData.partitionTime;
              loadDataRender(initPara,true);
            }else{
              loadDataRender(initPara);
            }

          });
        }else{
          loadDataRender(initPara);
        }

      }
    },$scope.state.mode=='view'?1:400)

  });
  //snow part
  //column table info
  var container=$(".tableColumnInfoBox table").eq(1).parent();
  $scope.showColumnInfo=function(tableName,columnName){
    $scope.getSampleDataByTableName(tableName,function(){
      $scope.tableName=tableName;
      $scope.columnName=columnName;
      $scope.columnType= TableModel.columnNameTypeMap[VdmUtil.removeNameSpace(columnName)]||''
      $scope.columnList=TableModel.getColumnsByTable(tableName);
      $scope.columnList.map(function(obj,k){
        if(obj.name==columnName){
          $scope.columnIndex=k;
          return false;
        }
      });
      var thColumn=$("#"+VdmUtil.removeNameSpace(columnName));
      if(thColumn&&thColumn.length){
        autoScroll(container,thColumn);
      }else{
        setTimeout(function(){
          container=$(".tableColumnInfoBox table").eq(1).parent();
          autoScroll(container,$("#"+VdmUtil.removeNameSpace(columnName)));
        },10)
      }
    });
  }
  //sampleData
  $scope.getSampleDataByTableName=function(tableName,callback){
    TableExtService.getSampleInfo({tableName:tableName},function(data){
      var sampleData=[],specialData=[];
      if(data.sample_rows&&data.sample_rows.length){
        sampleData=sampleData.concat(VdmUtil.changeDataAxis(data.sample_rows,true));
      }
      $scope.specialData=data.columns_stats;
      $scope.sampleData=sampleData;
      if(typeof callback=='function'){
        callback();
      }
    })
  }

  function autoScroll(container,scrollTo){
    if(scrollTo&&scrollTo.length){
      container.animate({
        scrollLeft: scrollTo.offset().left - container.offset().left + container.scrollLeft()
      });
      $(".tableColumnInfoBox table th").css('color','#fff');
      scrollTo.css('color','#3276b1');
    }
  }
  //snow





});
