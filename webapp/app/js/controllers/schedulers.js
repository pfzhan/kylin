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

KylinApp
  .controller('SchedulersCtrl', function ($scope, SchedulerJobService, $q, $routeParams, loadingRequest, kylinCommon, $interval, $modal, ProjectService, MessageService,SweetAlert,ProjectModel,$window) {

    $scope.schedulerJobList = [];

    $scope.list = function (offset, limit) {
      var _project = ProjectModel.selectedProject;
      offset = (!!offset) ? offset : 0;
      var jobRequest = {
        projectName: _project,
        offset: offset,
        limit: limit
      };

      $scope.schedulerJobList = [];
      $scope.state.loading = true;
      $scope.state.selectedScheduler = null;

      SchedulerJobService.list(jobRequest, function (schedulerList) {
        angular.forEach(schedulerList, function (scheduler) {
          $scope.schedulerJobList.push(scheduler);
        })
        $scope.state.loading = false;
      }, function (e) {
        $scope.state.loading = false;
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : $scope.dataKylin.alert.error_failed_to_load_schedulers;
          SweetAlert.swal($scope.dataKylin.alert.oops, msg, 'error');
        } else {
          SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.error_failed_to_load_schedulers, 'error');
        }
      });
    }

    $scope.remove = function (scheduler) {

      SweetAlert.swal({
        title: '',
        text: $scope.dataKylin.alert.tip_sure_to_delete,
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();

          SchedulerJobService.delete({name:scheduler.name},{},function(){
              loadingRequest.hide();
              kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_cube_drop);
              $scope.schedulerJobList.splice($scope.schedulerJobList.indexOf(scheduler),1);
              $scope.list();
            },function(e){

              loadingRequest.hide();
              kylinCommon.error_default(e);
            });
          }
      });
    };

    $scope.list();

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if(newValue!=oldValue||newValue==null){
        $scope.list();
      }

    });

  });
