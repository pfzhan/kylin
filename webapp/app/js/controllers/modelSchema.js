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

KylinApp.controller('ModelSchemaCtrl', function ($scope,$timeout, $routeParams,QueryService, UserService, ProjectService, $q, AuthenticationService, $filter, ModelService, MetaModel, CubeDescModel, CubeList, TableModel, ProjectModel, $log, SweetAlert, modelsManager,language, modelsEdit,DrawHelper,$modal,cubeConfig,loadingRequest,$location,ModelDescService) {

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
    }
  })
  // ~ init
  if (!$scope.state) {
    $scope.state = {mode: "view"};
  }

  if(!$scope.partitionColumn){
    $scope.partitionColumn ={
      "hasSeparateTimeColumn" : false
    }
  }

  $scope.dataKylin =  language.getDataKylin();
  $scope.wizardSteps =  [
    {title: $scope.dataKylin.model.schema[0], src: 'partials/modelDesigner/model_info.html', isComplete: false, form: 'model_info_form'},
    {title: $scope.dataKylin.model.schema[1], src: 'partials/modelDesigner/data_model.html', isComplete: false, form: 'data_model_form'},
    {
      title: $scope.dataKylin.model.schema[2],
      src: 'partials/modelDesigner/model_dimensions.html',
      isComplete: false,
      form: 'model_dimensions_form'
    },
    {
      title: $scope.dataKylin.model.schema[3],
      src: 'partials/modelDesigner/model_measures.html',
      isComplete: false,
      form: 'model_measure_form'
    },
    {
      title: $scope.dataKylin.model.schema[4],
      src: 'partials/modelDesigner/conditions_settings.html',
      isComplete: false,
      form: 'model_setting_form'
    }
  ];

  $scope.curStep = $scope.wizardSteps[0];

  $scope.$on("finish",function (event,data) {
    $scope.dataKylin =  language.getDataKylin();
    for(var i in $scope.wizardSteps){
      $scope.wizardSteps[i].title = $scope.dataKylin.model.schema[i];
    }
  });






  //snow

//snow part
  jsPlumb.ready(function() {

    //action=============================================


    //data===============================================
    var bData=[{
      'table':'Default.Kylin',
      'alias':'kylin',
      'kind':'fact',
      'join':{
        'type':'inner',
        'primary_key':['kylin.id'],
        'foreign_key':['kylin.userName']
      }
    }];
    //data======================================

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
          treeDom.sortable('cancel') ;
          var database=$(ui.item).attr("data");
          TableModel.initTableData(function(){
            var tableDataList=TableModel.getTableDatail(database);
            if(tableDataList&&tableDataList.length<2){
              var table=tableDataList[0];
              ModelService.suggestion({
                table:table.database+'.'+table.name
              },function(data){
                for(var i=0;i<table.columns.length;i++){
                  for(var m in data){
                    if(m.indexOf(table.columns[i].name)>=0){
                      table.columns[i].kind=data[m].replace(/_[^_\s]+$/,'');
                      break;
                    }
                  }
                }
                DrawHelper.addTable(table);

              },function(){
                DrawHelper.addTable(table);
              })
            }
          })
        }
      })
      //修改snowTable信息弹窗
      function openSnowTableInfoDialog(table,callback){
        var snowTableInfoCtrl=function($scope, $modalInstance,table){
          if(typeof callback=='function'){
            table.alias='';
          }
          $scope.table=table;
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
          var oldAlias=table.alias;
          if(DrawHelper.tableList.getTable('guid',table.guid)){
            $scope.showRemoveBtn=true;
          }
          $scope.editTableInfo=function(){
            //监测是否有重复的别名
            if(oldAlias!=table.alias&&DrawHelper.checkHasThisAlias(table.alias)){
              $scope.sameError=true;
              return;
            }
            var rootFactTable=DrawHelper.tableList.getRootFact();
            if(table.kind=='ROOTFACT'&&rootFactTable&&rootFactTable.guid!=table.guid){
              $scope.hasRootfactError=true;
              return;
            }
            if(table.kind=='ROOTFACT'){
              if(DrawHelper.getForeignKeyCount(table.guid)>0){
                $scope.hasForeignKeysError=true;
                return;
              }
              table.alias=table.name;
            }
            //更新 数据仓库里的别名和表类型
            DrawHelper.tableList.update('table',table.guid,{
              alias:table.alias,
              kind:table.kind
            });
            DrawHelper.changeAliasList(oldAlias,table.alias);
            if(typeof callback=='function'){
              callback(table);
            }else{
              //更新 界面的的别名和表类型
              DrawHelper.refreshAlias(table.guid,table.alias);
              DrawHelper.refreshTableKind(table.guid,table.kind);

            }
            $modalInstance.dismiss('cancel');
          }
        }
        var modalInstance = $modal.open({
          windowClass:'modal_snowtableInfo',
          templateUrl: 'partials/models/snowmodel_info.html',
          controller: snowTableInfoCtrl,
          backdrop: 'static',
          resolve:{
            table:function() {
              return table;
            }
          }
        });
      }

      //选择join类型弹窗
      function openSelectJoinTypeDialog(conn){
        var snowTableJoinTypeCtrl=function($scope,$modalInstance,conn){
          var linkData=DrawHelper.connects[conn.id];
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
            }
          }
        });
      }


      //选择date类型弹窗
      function openDateFormatTypeDialog(callback,whatDate){
        var dateFormatTypeCtrl=function($scope,$modalInstance,callback,cubeConfig,whatDate){
          $scope.callback=callback;
          $scope.formatDateType='yyyyMMdd';
          $scope.formatTimeType='HH:mm:ss'
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
              $scope.callback($scope.formatDateType);
            }else{
              $scope.callback($scope.formatTimeType);
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
            }
          }
        });
      }
      //弹出json内容
      function openJsonDialog(){
        var snowJsonCtr=function($scope,$modalInstance){
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

          }
        });
      }
//model 保存弹窗
      function openModelSaveDialog(conn){
        var snowModelSaveCtrl=function($scope,$modalInstance,SweetAlert,VdmUtil,scope,MessageService,loadingRequest,$location){
         if(DrawHelper.kylinData){
           $scope.model={name:DrawHelper.kylinData.name, description:DrawHelper.kylinData.description, filter:DrawHelper.kylinData.filter_condition};
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
          $scope.saveModel=function(){
            DrawHelper.instanceName=$scope.model.name;
            DrawHelper.instanceDiscribe=$scope.model.description;
            DrawHelper.filterStr=$scope.model.filter;
            var saveData;
            try {
              saveData=angular.fromJson(DrawHelper.plumbDataToKylinData());
            } catch (e) {
              SweetAlert.swal(scope.dataKylin.alert.oops, scope.dataKylin.alert.tip_invalid_model_json, 'error');
              return;
            }
            if(DrawHelper.kylinData){
               var updateModelData= $.extend({},DrawHelper.kylinData,saveData);
               ModelService.update({}, {
                modelDescData:VdmUtil.filterNullValInObj(updateModelData),
                modelName: saveData.name,
                project:scope.state.project
              }, function (request) {
                if (request.successful) {
                  //$scope.state.modelSchema = request.modelSchema;
                  $modalInstance.dismiss('cancel');
                  SweetAlert.swal('', scope.dataKylin.alert.success_updated_model, 'success');
                  $location.path("/models");
                  //location.reload();
                } else {
                  //$scope.saveModelRollBack();
                  var message =request.message;
                  var msg = !!(message) ? message : scope.dataKylin.alert.error_info;
                  MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':''}), 'error', {}, true, 'top_center');
                }
                //end loading
                loadingRequest.hide();
              }, function (e) {
                //$scope.saveModelRollBack();

                if(e.data&& e.data.exception){
                  var message =e.data.exception;
                  var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;

                  MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':''}), 'error', {}, true, 'top_center');
                } else {
                  MessageService.sendMsg($scope.modelResultTmpl({'text':scope.dataKylin.alert.error_info,'schema':''}), 'error', {}, true, 'top_center');
                }
                loadingRequest.hide();
              });
            }else{
              ModelService.save({}, {
                modelDescData:VdmUtil.filterNullValInObj(saveData),
                project: scope.state.project
              }, function (request) {
                if(request.successful) {
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
      }
      //拖拽建模（雪花）初始化入口
      DrawHelper=DrawHelper.initService();
      if ($scope.isEdit = !!$routeParams.modelName||$scope.state.mode=='view') {

        var modelName = $routeParams.modelName||$scope.state.modelName;

        ModelDescService.query({model_name: modelName}, function (model) {
          if (model) {
            $scope.modelCopy = angular.copy(model);
            modelsManager.setSelectedModel($scope.modelCopy);
            modelsEdit.setSelectedModel(model);
            $scope.lookupLength=modelsEdit.selectedModel.lookups.length;

            DrawHelper.init({
              changeTableInfo:openSnowTableInfoDialog,
              changeConnectType:openSelectJoinTypeDialog,
              addColumnToPartitionDate:openDateFormatTypeDialog,
              addColumnToPartitionTime:openDateFormatTypeDialog,
              saveModel:openModelSaveDialog,
              kylinData:$scope.modelCopy,
              openJsonDialog:openJsonDialog,
              actionLock:$scope.state.mode=='view'
            });
            DrawHelper.kylinDataToJsPlumbData();

          }
        });
        //init project
      }else{
        DrawHelper.init({
          changeTableInfo:openSnowTableInfoDialog,
          changeConnectType:openSelectJoinTypeDialog,
          addColumnToPartitionDate:openDateFormatTypeDialog,
          addColumnToPartitionTime:openDateFormatTypeDialog,
          saveModel:openModelSaveDialog,
          openJsonDialog:openJsonDialog
        });
      }

    },1)

  });









  //snow part

  //snow






  //init modelsManager
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

  $scope.$watch('model', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if ($scope.modelMode == "editExistModel" && newValue && !newValue.project) {
      initProject();
    }

  });

  // ~ public methods
  $scope.filterProj = function (project) {
    return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project, $scope.permissions.ADMINISTRATION.mask);
  };

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.open = function ($event) {
    $event.preventDefault();
    $event.stopPropagation();

    $scope.opened = true;
  };
  $scope.preView = function () {
    var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
    if (stepIndex >= 1) {
      $scope.curStep.isComplete = false;
      $scope.curStep = $scope.wizardSteps[stepIndex - 1];
    }
  };

  $scope.nextView = function () {
    var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
    if (stepIndex < ($scope.wizardSteps.length - 1)) {
      $scope.curStep.isComplete = true;
      $scope.curStep = $scope.wizardSteps[stepIndex + 1];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  };

  $scope.checkForm = function (stepIndex) {
    // do not check for Prev Step
    if (stepIndex + 1 < $scope.curStep.step) {
      return true;
    }

    if (!$scope.curStep.form) {
      return true;
    }
    if ($scope.state.mode === 'view') {
      return true;
    }
    else {
      //form validation
      if ($scope.forms[$scope.curStep.form].$invalid) {
        $scope.forms[$scope.curStep.form].$submitted = true;
        return false;
      } else {
        //business rule check
        switch ($scope.curStep.form) {
          case 'data_model_form':
            return $scope.check_data_model();
            break;
          case 'model_info_form':
            return $scope.check_model_info();
            break;
          case 'model_dimensions_form':
            return $scope.check_model_dimensions();
            break;
          case 'model_measure_form':
            return $scope.check_model_measure();
            break;
          case 'model_setting_form':
            return $scope.check_model_setting();
            break;
          default:
            return true;
            break;
        }
      }
    }
  };

  $scope.check_model_info = function () {

    var modelName = $scope.modelsManager.selectedModel.name.toUpperCase();
    var models = $scope.modelsManager.modelNameList;
    if ($scope.modelMode=="addNewModel") {
      if(models.indexOf(modelName) != -1 || models.indexOf(modelName.toLowerCase()) !=-1){
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.warning_model_part_one + modelName + $scope.dataKylin.alert.warning_model_part_two, 'warning');
        return false;
      }
    }
    return true;
  }

  /*
   * lookups can't be null
   */
  $scope.check_data_model = function () {
    var errors = [];
//        if(!modelsManager.selectedModel.lookups.length){
//            errors.push("No lookup table defined");
//        }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  }

  /*
   * dimensions validation
   * 1.dimension can't be null
   */
  $scope.check_model_dimensions = function () {

    var errors = [];
    if (!modelsManager.selectedModel.dimensions.length) {
      errors.push("No dimensions defined.");
    }
    var isDimensionDefined = false;
    angular.forEach(modelsManager.selectedModel.dimensions, function (_dimension) {
      if(_dimension.columns && _dimension.columns.length){
        isDimensionDefined = true;
      }
    });
    if(!isDimensionDefined){
      errors.push($scope.dataKylin.alert.check_model_dimension);
    }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal($scope.dataKylin.alert.oops, errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  };

  /*
   * dimensions validation
   * 1.metric can't be null
   */
  $scope.check_model_measure = function () {

    var errors = [];
    if (!modelsManager.selectedModel.metrics || !modelsManager.selectedModel.metrics.length) {
      errors.push($scope.dataKylin.alert.check_model_measure_define);
    }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  };
  $scope.check_model_setting = function () {
    var errors = [];
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }
  }


  $scope.goToStep = function (stepIndex) {
    if ($scope.modelMode == "addNewModel") {
      /*if (stepIndex + 1 >= $scope.curStep.step) {
        return;
      }*/
    }
    for (var i = 0; i < $scope.wizardSteps.length; i++) {
      if (i <= stepIndex) {
        $scope.wizardSteps[i].isComplete = true;
      } else {
        $scope.wizardSteps[i].isComplete = false;
      }
    }
    if (stepIndex < ($scope.wizardSteps.length)) {
      $scope.curStep = $scope.wizardSteps[stepIndex];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  }

  // ~ private methods
  function initProject() {

  }











});
