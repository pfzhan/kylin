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

KylinApp.controller('ExtFilterCtrl', function ($scope,$rootScope,$modal, ExtFilterService,AccessService, MessageService, AuthenticationService, SweetAlert) {
  $scope.extFilters =[];
  $scope.filterProject = {};

  var param = {
    project:$scope.project.name
  }


  $scope.list = function(){
    ExtFilterService.list(param,function (filters){
      $scope.extFilters = filters;
    },function(){
      SweetAlert.swal('', 'Failed to load external filters.', 'error');

    });
  }

  $scope.list();

  $scope.toCreateFilter = function (filters) {
    $modal.open({
      templateUrl: 'projectFilter.html',
      controller: filterCreateCtrl,
      resolve: {
        projects: function () {
          return null;
        },
        extfilter: function () {
          return null;
        }
      }
    });
  };

  $scope.toEditFilter = function (filters,_filter) {
    $modal.open({
      templateUrl: 'projectFilter.html',
      controller: filterCreateCtrl,
      resolve: {
        projects: function () {
          return null;
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
      text: 'Are you sure to delete ?',
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm){
        ExtFilterService.remove({filterName: filter.name,projectName:project.name}, function(){
          $rootScope.$emit('extFiltersUpdated');
          SweetAlert.swal('Success!',"Filter [" + filter.name + "] has been deleted successfully!", 'success');
        },function(e){
          if(e.data&& e.data.exception){
            var message =e.data.exception;
            var msg = !!(message) ? message : 'Failed to take action.';
            SweetAlert.swal('Oops...', msg, 'error');
          }else{
            SweetAlert.swal('Oops...', "Failed to take action.", 'error');
          }
        });
      }
    });
  }

  $rootScope.$on('extFiltersUpdated', function(event, data) {
    $scope.list();
  });



});




var filterCreateCtrl = function ($scope,$rootScope,ExtFilterService,cubeConfig, $location, $modalInstance, ProjectService, MessageService, projects, extfilter, SweetAlert, ProjectModel) {

  $scope.cubeConfig = cubeConfig;
  $scope.state={
    isEdit:false
  }
  $scope.projectFilter = {
    name:"",
    filter_resource_identifier:"",
    filter_table_type:"",
    description:""
  }

  if(extfilter !== null){
    $scope.state.isEdit = true;
    $scope.projectFilter = $.extend(true,{},extfilter);
  }


  var _project = ProjectModel.selectedProject;

  $scope.createOrUpdate = function () {
    var filterData = angular.toJson($scope.projectFilter,true);
    if ($scope.state.isEdit) {
      ExtFilterService.update({}, {
        project:_project,
        extFilter:filterData
      }, function (e) {
        $rootScope.$emit('extFiltersUpdated');
        SweetAlert.swal('Success!', 'Filter [' + $scope.projectFilter.name + '] updated successfully!', 'success');
        $modalInstance.dismiss('cancel');
      }, function (e) {
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : 'Failed to take action.';
          SweetAlert.swal('Oops...', msg, 'error');
        } else {
          SweetAlert.swal('Oops...', "Failed to take action.", 'error');
        }
      });
    }
    else {
      var filterData = angular.toJson($scope.projectFilter,true);
      ExtFilterService.save({}, {
        project:_project,
        extFilter:filterData
      }, function (e) {
        $rootScope.$emit('extFiltersUpdated');
        SweetAlert.swal('Success!', 'New Filter [' + $scope.projectFilter.name + '] created successfully!', 'success');
        $modalInstance.dismiss('cancel');
      }, function (e) {
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : 'Failed to take action.';
          SweetAlert.swal('Oops...', msg, 'error');
        } else {
          SweetAlert.swal('Oops...', "Failed to take action.", 'error');
        }
      });
    }
  };



  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };


};
