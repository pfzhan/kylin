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

KylinApp.controller('ExtFilterCtrl', function ($scope,$rootScope,$modal, ExtFilterService,AccessService, MessageService, AuthenticationService, SweetAlert,kylinCommon) {
  $scope.extFilters =[];
  $scope.filterProject = {};

  var param = {
    project:$scope.project.name
  }


  $scope.list = function(){
    ExtFilterService.list(param,function (filters){
      $scope.extFilters = filters;
    },function(){
      SweetAlert.swal('', $scope.dataKylin.alert.error_info, 'error');

    });
  }

  $scope.list();

  $scope.toCreateFilter = function (targetProject,filters) {
    $modal.open({
      templateUrl: 'projectFilter.html',
      controller: filterCreateCtrl,
      resolve: {
        projects: function () {
          return null;
        },
        project:function(){
          return targetProject;
        },
        extfilter: function () {
          return null;
        }
      }
    });
  };

  $scope.toEditFilter = function (targetProject,filters,_filter) {
    $modal.open({
      templateUrl: 'projectFilter.html',
      controller: filterCreateCtrl,
      resolve: {
        projects: function () {
          return null;
        },
        project:function(){
          return targetProject;
        },
        extfilter: function () {
          return _filter;
        }
      }
    });
  };


  $scope.deleteFilter= function(filters,filter,project){
    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_delete_filter,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        ExtFilterService.remove({filterName: filter.name,projectName:project.name}, function(){
          $rootScope.$emit('extFiltersUpdated');
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_delete_filter_part_one+ filter.name + $scope.dataKylin.tip_delete_filter_part_two);
        },function(e){
          kylinCommon.error_default(e);
        });
      }
    });
  }

  $rootScope.$on('extFiltersUpdated', function(event, data) {
    $scope.list();
  });



});




var filterCreateCtrl = function ($scope,$rootScope,ExtFilterService,cubeConfig, $location, $modalInstance, ProjectService, MessageService, projects,project, extfilter, SweetAlert, ProjectModel,language,kylinCommon) {
  $scope.dataKylin = language.getDataKylin();
  $scope.cubeConfig = cubeConfig;
  $scope.state={
    isEdit:false
  }
  $scope.projectFilter = {
    name:"",
    filter_resource_identifier:"",
    filter_table_type:"HDFS",
    description:""
  }

  if(extfilter !== null){
    $scope.state.isEdit = true;
    $scope.projectFilter = $.extend(true,{},extfilter);
  }


  var _project = project;

  $scope.createOrUpdate = function () {
    var filterData = angular.toJson($scope.projectFilter,true);
    if ($scope.state.isEdit) {
      ExtFilterService.update({}, {
        project:_project,
        extFilter:filterData
      }, function (e) {
        $rootScope.$emit('extFiltersUpdated');
        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_delete_filter_part_one + $scope.projectFilter.name + $scope.dataKylin.alert.tip_update_filter_part_two);
        $modalInstance.dismiss('cancel');
      }, function (e) {
        kylinCommon.error_default(e);
      });
    }
    else {
      var filterData = angular.toJson($scope.projectFilter,true);
      ExtFilterService.save({}, {
        project:_project,
        extFilter:filterData
      }, function (e) {
        $rootScope.$emit('extFiltersUpdated');
        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_create_filter_part_one+$scope.projectFilter.name+$scope.dataKylin.alert.tip_create_filter_part_two);
        SweetAlert.swal($scope.dataKylin.alert.success, $scope.dataKylin.alert.success_filecreat_part_one + $scope.projectFilter.name + $scope.dataKylin.alert.success_filecreat_part_two, 'success');
        $modalInstance.dismiss('cancel');
      }, function (e) {
        kylinCommon.error_default(e);
      });
    }
  };



  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };


};
