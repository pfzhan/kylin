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
    .controller('JobCtrl', function ($scope, $q, $routeParams, $interval, $modal, ProjectService, MessageService, JobService,SweetAlert,loadingRequest,UserService,jobConfig,JobList,$window,language,kylinCommon) {
        $scope.initJobData = function(){
          $scope.dataKylin.monitor.jobConfig = language.getLanguageType()==0?jobConfig.dataEnglish:jobConfig.dataChinese;//init data depend on English or Chinese
          $scope.jobConfig = $scope.dataKylin.monitor.jobConfig;
          $scope.timeFilter = $scope.dataKylin.monitor.jobConfig.timeFilter[1];
        };
      $scope.$on("finish",function (event,data) {
        $scope.initJobData();
      });
        $scope.initJobData();
        $scope.jobList = JobList;
        JobList.removeAll();
        $scope.cubeName = null;
        //$scope.projects = [];
        $scope.action = {};
        $scope.status = [];
        $scope.toggleSelection = function toggleSelection(current) {
            var idx = $scope.status.indexOf(current);
            if (idx > -1) {
              $scope.status.splice(idx, 1);
            }else {
              $scope.status.push(current);
            }
        };


      //cubes,models,data source
      $scope.tabs=[
        {
          "title":"Jobs",
          "active":true
        },
        {
          "title": "Slow Queries",
          "active": false
        },
        {
          "title": "Scheduled Jobs",
          "active": false
        }
      ]

        // projectName from page ctrl
        $scope.state = {loading: false, refreshing: false, filterAttr: 'last_modified', filterReverse: true, reverseColumn: 'last_modified', projectName:$scope.projectModel.selectedProject};
        var jobRequest;
        $scope.list = function (offset, limit) {
            var defer = $q.defer();
            if(!$scope.projectModel.projects.length){
                defer.resolve([]);
              return  defer.promise;
            }
            offset = (!!offset) ? offset : 0;

            var statusIds = [];
            angular.forEach($scope.status, function (statusObj, index) {
                statusIds.push(statusObj.value);
            });

            $scope.cubeName=$scope.cubeName == ""?null:$scope.cubeName;

            jobRequest = {
                cubeName: $scope.cubeName,
                projectName: $scope.state.projectName,
                status: statusIds,
                offset: offset,
                limit: limit,
                timeFilter: $scope.timeFilter.value
            };
            $scope.state.loading = true;

            return JobList.list(jobRequest).then(function(resp){
                $scope.state.loading = false;
                if (angular.isDefined($scope.state.selectedJob)) {
                    $scope.state.selectedJob = JobList.jobs[ $scope.state.selectedJob.uuid];
                }
                defer.resolve(resp);
                return defer.promise;
            },function(resp){
              $scope.state.loading = false;
              defer.resolve([]);
              SweetAlert.swal($scope.dataKylin.alert.oops, resp, 'error');
              return defer.promise;
            });
        }

        $scope.reload = function () {
            // trigger reload action in pagination directive
            $scope.action.reload = !$scope.action.reload;
        };
        $scope.calcObjLen=function(obj){
          var i=0;
          for(var s in obj){
            i++;
          }
          return i;
        }
        //定时刷状态
        function asynProgress(){
          setTimeout(function(){
            var statusIds = [];
            angular.forEach($scope.status, function (statusObj, index) {
              statusIds.push(statusObj.value);
            });
            var newJobPara= $.extend({}, {
              status: statusIds,
              projectName: $scope.projectModel.selectedProject,
              timeFilter: $scope.timeFilter.value
            },{offset:0,limit:$scope.calcObjLen($scope.jobList.jobs)});
            JobService.list(newJobPara,function(data){
              for(var job in $scope.jobList.jobs){
                for(var i =0;i<data.length;i++){
                  if(data[i].uuid==job){
                    $scope.jobList.jobs[job]= angular.extend($scope.jobList.jobs[job],data[i]);
                    break;
                  }
                }
              }
              if(location.pathname=="/kylin/jobs"){
                asynProgress();
              }
            },function(){
              if(location.pathname=="/kylin/jobs"){
                asynProgress();
              }
            })
          },5000)
        }
        if(location.pathname=="/kylin/jobs"){
           asynProgress();
        }
        $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
            if(newValue!=oldValue||newValue==null){
                JobList.removeAll();
                $scope.state.projectName = newValue;
                $scope.reload();
            }

        });
        $scope.resume = function (job) {
            SweetAlert.swal({
                title: '',
                text: $scope.dataKylin.alert.tip_to_resume_job,
                type: '',
                showCancelButton: true,
                confirmButtonColor: '#DD6B55',
                confirmButtonText: "Yes",
                closeOnConfirm: true
            }, function(isConfirm) {
              if(isConfirm) {
                loadingRequest.show();
                JobService.resume({jobId: job.uuid}, {}, function (job) {
                  loadingRequest.hide();
                  JobList.jobs[job.uuid] = job;
                  if (angular.isDefined($scope.state.selectedJob)) {
                    $scope.state.selectedJob = JobList.jobs[$scope.state.selectedJob.uuid];
                  }
                  kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_job_been_resumed);
                }, function (e) {
                  loadingRequest.hide();
                  kylinCommon.error_default(e);
                });
              }
            });
        }
        $scope.pause = function (job) {
              SweetAlert.swal({
                  title: '',
                  text: 'Are you sure to pause the job?',
                  type: '',
                  showCancelButton: true,
                  confirmButtonColor: '#DD6B55',
                  confirmButtonText: "Yes",
                  closeOnConfirm: true
              }, function(isConfirm) {
                if(isConfirm) {
                    loadingRequest.show();
                    JobService.pause({jobId: job.uuid}, {}, function (job) {
                        loadingRequest.hide();
                        $scope.safeApply(function() {
                            JobList.jobs[job.uuid] = job;
                           if (angular.isDefined($scope.state.selectedJob)) {
                                $scope.state.selectedJob = JobList.jobs[ $scope.state.selectedJob.uuid];
                              }

                            });
                        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_Job_been_paused);
                      },function(e){
                        loadingRequest.hide();
                        kylinCommon.error_default(e);
                      });
                  }
              });
          }

        $scope.cancel = function (job) {
            SweetAlert.swal({
                title: '',
                text: $scope.dataKylin.alert.tip_to_discard_job,
                type: '',
                showCancelButton: true,
                confirmButtonColor: '#DD6B55',
                confirmButtonText: "Yes",
                closeOnConfirm: true
            }, function(isConfirm) {
              if(isConfirm) {
                loadingRequest.show();
                JobService.cancel({jobId: job.uuid}, {}, function (job) {
                    loadingRequest.hide();
                    $scope.safeApply(function() {
                        JobList.jobs[job.uuid] = job;
                        if (angular.isDefined($scope.state.selectedJob)) {
                            $scope.state.selectedJob = JobList.jobs[ $scope.state.selectedJob.uuid];
                        }

                    });
                    kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_Job_been_discarded);
                },function(e){
                    loadingRequest.hide();
                    kylinCommon.error_default(e);
                });
              }
            });
        }


      $scope.diagnosisJob =function(job) {
        if (!job){
          SweetAlert.swal('', $scope.dataKylin.alert.tip_no_job_selected, 'info');
          return;
        }
        var downloadUrl = Config.service.url + 'diag/job/'+job.uuid+'/download';
        $window.open(downloadUrl);
      }


        $scope.openModal = function () {
          $scope.selectStepDetail={};
            if (angular.isDefined($scope.state.selectedStep)) {
                if ($scope.state.stepAttrToShow == "output") {
                    $scope.state.selectedStep.loadingOp = true;
                    internalOpenModal();
                    var stepId = $scope.state.selectedStep.sequence_id;
                    JobService.stepOutput({jobId: $scope.state.selectedJob.uuid, propValue: $scope.state.selectedStep.id}, function (result) {
                        if (angular.isDefined(JobList.jobs[result['jobId']])) {
                            var tjob = JobList.jobs[result['jobId']];
                            tjob.steps[stepId].cmd_output = result['cmd_output'];
                            tjob.steps[stepId].loadingOp = false;
                          $scope.selectStepDetail.cmd_output=result['cmd_output'];
                          $scope.selectStepDetail.loadingOp=false;

                        }
                    },function(e){
                      SweetAlert.swal($scope.dataKylin.alert.oops,$scope.dataKylin.alert.error_failed_to_load_job, 'error');
                    });
                } else {
                   $scope.selectStepDetail.exec_cmd=$scope.state.selectedStep.exec_cmd;
                    internalOpenModal();
                }
            }
        }

        function internalOpenModal() {
            $modal.open({
                templateUrl: 'jobStepDetail.html',
                controller: jobStepDetail,
                resolve: {
                    step: function () {
                      return $scope.selectStepDetail;
                      //return $scope.state.selectedStep;
                    },
                    attr: function () {
                        return $scope.state.stepAttrToShow;
                    }
                }
            });
        }
    }
);

var jobStepDetail = function ($scope, $modalInstance, step, attr,language) {
    $scope.dataKylin = language.getDataKylin();//get datakYLIN;
    $scope.step = step;
    $scope.stepAttrToShow = attr;
    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    }
};
