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

KylinApp.controller('ModelsCtrl', function ($scope, $q, $routeParams, $location, $window, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, modelConfig, ProjectModel, ModelService, MetaModel, modelsManager, cubesManager, TableModel, AccessService,language,kylinCommon,VdmUtil) {

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
  $scope.action = {};

  //trigger init with directive []
  $scope.list = function (offset, limit) {
    var defer = $q.defer();
    offset = (!!offset) ? offset : 0;
    limit = (!!limit) ? limit : 20;
    var queryParam = {offset: offset, limit: limit};
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
      defer.resolve(resp.length);
      modelsManager.loading = false;
      return defer.promise;
    });

  };


  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if (newValue != oldValue || newValue == null) {
      modelsManager.removeAll();
      $scope.reload();
    }

  });
  $scope.reload = function () {
    // trigger reload action in pagination directive
    $scope.action.reload = !$scope.action.reload;
  };
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
          modelsManager.models.splice(modelsManager.models.indexOf(model),1);
        }, function (e) {
          loadingRequest.hide();
          kylinCommon.error_default(e);
        });
      }

    });
  };

  $scope.statsModel=function(model){
    ModelService.stats({propName: model.project,propValue:model.name,action:'stats'}, {}, function (result) {
      loadingRequest.hide();
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.collectStaticsSuccess);
    }, function (e) {
      loadingRequest.hide();
      kylinCommon.error_default(e);
    });
  }
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
    CubeService.list({modelName:model.name}, function (_cubes) {
      model.cubes = _cubes;
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
    })
  };


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

  $scope.listModelAccess = function (model) {
    if(model.uuid){
      AccessService.list({type: "DataModelDesc", uuid: model.uuid}, function (accessEntities) {
        model.accessEntities = accessEntities;
        try {
          if (!model.owner) {
            model.owner = accessEntities[0].sid.principal;
          }
        } catch (error) {
          $log.error("No acl info.");
        }
      })
    }
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
    targetProject:model.project
  }
  console.log(model);
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
