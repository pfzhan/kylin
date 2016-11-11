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
