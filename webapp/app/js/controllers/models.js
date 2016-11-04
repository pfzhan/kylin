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

'use strict';

KylinApp.controller('ModelsCtrl', function ($scope, $q, $routeParams, $location, $window, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, modelConfig, ProjectModel, ModelService, MetaModel, modelsManager, cubesManager, TableModel, $animate,language,kylinCommon,VdmUtil) {

  //tree data

  $scope.cubeSelected = false;
  $scope.cube = {};

  $scope.showModels = true;

  $scope.state = {
    filterAttr: 'create_time', filterReverse: true, reverseColumn: 'create_time',
    dimensionFilter: '', measureFilter: ''
  };

  //cubes,models,data source
  $scope.tabs=[
    {
      "title":"Data Source",
      "active":false
    },
    {
      "title":"Models",
      "active":false
    },
    {
      "title":"Cubes",
      "active":false
    }
  ]


  if($routeParams.tab){
      for(var i=0;i<$scope.tabs.length;i++){
        if($scope.tabs[i].title==$routeParams.tab){
          $scope.tabs[i]["active"]=true;
          break;
        }
      }
  }else{
    $scope.tabs[2].active=true;
  }
  //tracking data loading status in /models page
  $scope.tableModel = TableModel;

  $scope.toggleTab = function (showModel) {
    $scope.showModels = showModel;
  }
  $scope.modelsManager = modelsManager;
  $scope.cubesManager = cubesManager;
  $scope.modelConfig = modelConfig;
  modelsManager.removeAll();
  $scope.loading = false;
  $scope.window = 0.68 * $window.innerHeight;


  //trigger init with directive []
  $scope.list = function () {
    var defer = $q.defer();
    var queryParam = {};
    if (!$scope.projectModel.isSelectedProjectValid()) {
      defer.resolve([]);
      return defer.promise;
    }

    if (!$scope.projectModel.projects.length) {
      defer.resolve([]);
      return defer.promise;
    }
    queryParam.projectName = $scope.projectModel.selectedProject;
    return modelsManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      modelsManager.loading = false;
      return defer.promise;
    });

  };

  $scope.list();

  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if (newValue != oldValue || newValue == null) {
      $scope.list();
    }

  });

  $scope.status = {
    isopen: true
  };

  $scope.toggled = function (open) {
    $log.log('Dropdown is now: ', open);
  };

  $scope.toggleDropdown = function ($event) {
    $event.preventDefault();
    $event.stopPropagation();
    $scope.status.isopen = !$scope.status.isopen;
  };

  $scope.hideSideBar = false;
  $scope.toggleModelSideBar = function () {
    $scope.hideSideBar = !$scope.hideSideBar;
  }

  $scope.dropModel = function (model) {

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_drop_model,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        ModelService.drop({modelId: model.name}, {}, function (result) {
          loadingRequest.hide();
//                    CubeList.removeCube(cube);
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_model_drop_done);
          //location.reload();
        }, function (e) {
          loadingRequest.hide();
          kylinCommon.error_default(e);
        });
      }

    });
  };

  $scope.cloneModel = function(model){
    $scope.hasOpen=$modal.open({
      templateUrl: 'modelClone.html',
      controller: modelCloneCtrl,
      windowClass:"clone-cube-window",
      scope:$scope,
      resolve: {
        model: function () {
          return model;
        }
      }
    });
  }

  $scope.modelEdit = function (model) {

    var cubename = [];
    var i = 0;
    var modelstate = false;
    if (model.cubes.length != 0) {
      angular.forEach(model.cubes, function (cube) {
        if (cube.status == "READY") {
          modelstate = true;
          cubename[i] = cube.name;
          i++;
        }
      })
    }
    if (modelstate == false) {
      $location.path("/models/edit/" + model.name);
    }
    else {
      SweetAlert.swal($scope.dataKylin.alert.oops,$scope.dataKylin.alert.tip_model_be_used + cubename.join(',') +$scope.dataKylin.alert.tip_model_be_used_by,'warning');
    }
  }


  $scope.openModal = function (model) {
    $scope.modelsManager.selectedModel = model;
    $modal.open({
      templateUrl: 'modelDetail.html',
      controller: ModelDetailModalCtrl,
      resolve: {
        scope: function () {
          return $scope;
        }
      }
    });
  };

  var ModelDetailModalCtrl = function ($scope, $location, $modalInstance, scope) {
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
  };

  VdmUtil.storage.set("tsCache",true);
  if($location.path()=="/models/fromadd"){
      if(VdmUtil.storage.getObject(ProjectModel.getSelectedProject())&&VdmUtil.storage.getObject(ProjectModel.getSelectedProject()).name){
        SweetAlert.swal({
          title: '',
          text: $scope.dataKylin.alert.tip_cubadd_cache,
          type: 'info',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function (isConfirm) {
          if (isConfirm) {
            $location.path("/cubes/add");
          }else{
            VdmUtil.storage.remove(ProjectModel.getSelectedProject());
            VdmUtil.storage.remove(ProjectModel.getSelectedProject(ProjectModel.getSelectedProject()+"_rawtable"));
          }
          VdmUtil.storage.remove("tsCache");
        });
      }else{
        $location.path("/models");
      }
  }

});


var modelCloneCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, model, MetaModel, SweetAlert,ProjectModel, loadingRequest,ModelService,language,kylinCommon) {
  $scope.projectModel = ProjectModel;
  $scope.dataKylin = language.getDataKylin();
  $scope.targetObj={
    modelName:model.name+"_clone",
    targetProject:$scope.projectModel.selectedProject
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.cloneModel = function(){

    if(!$scope.targetObj.targetProject){
      SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_target_project, 'info');
      return;
    }
    $scope.modelRequest = {
      modelName:$scope.targetObj.modelName,
      project:$scope.targetObj.targetProject
    }

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_clone_model,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        ModelService.clone({modelId: model.name}, $scope.modelRequest, function (result) {
          loadingRequest.hide();
          SweetAlert.swal({title:$scope.dataKylin.alert.success, text:$scope.dataKylin.alert.success_clone_model,type: 'success'},function(){
             $scope.hasOpen.close();
          });
          $scope.list();
        }, function (e) {
          loadingRequest.hide();
          kylinCommon.error_default(e);
        });
      }
    });
  }

}
