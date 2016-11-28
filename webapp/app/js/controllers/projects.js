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
    .controller('ProjectCtrl', function ($scope,ExtFilterService, $modal, $q, ProjectService, MessageService,SweetAlert,$log,kylinConfig,projectConfig,ProjectModel,kylinCommon, AdminStoreService, VdmUtil) {

        $scope.projects = [];
        $scope.loading = false;
        $scope.projectConfig = projectConfig;

        $scope.state = { filterAttr: 'name', filterReverse: true, reverseColumn: 'name'};

        $scope.list = function (offset, limit) {
            offset = (!!offset) ? offset : 0;
            limit = (!!limit) ? limit : 20;
            var defer = $q.defer();
            var queryParam = {offset: offset, limit: limit};

            $scope.loading = true;
            ProjectService.listReadable(queryParam, function (projects) {
                $scope.projects = $scope.projects.concat(projects);
                angular.forEach(projects, function (project) {
                    $scope.listAccess(project, 'ProjectInstance');
                });
                $scope.loading = false;
                defer.resolve(projects.length);
            });

            return defer.promise;
        }

        $scope.toEdit = function(project) {
            $modal.open({
                templateUrl: 'project.html',
                controller: projCtrl,
                resolve: {
                    projects: function () {
                        return $scope.projects;
                    },
                    project: function(){
                        return project;
                    }
                }
            });
        }

        $scope.delete = function(project){
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
                ProjectService.delete({projecId: project.name}, function(){
                    var pIndex = $scope.projects.indexOf(project);
                    if (pIndex > -1) {
                        $scope.projects.splice(pIndex, 1);
                    }
                ProjectModel.removeProject(project.name);
                  kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_project_delete_part_one+project.name+$scope.dataKylin.alert.tip_project_delete_part_two);
                },function(e){
                    kylinCommon.error_default(e);
                });
                }
            });
        }

        $scope.backup = function(_project){
            AdminStoreService.globalBackup({}, {project:_project.name} , function(data){
                kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_store_callback+" "+VdmUtil.linkArrObjectToString(data));
            },function(e){
                kylinCommon.error_default(e);
            });
        }

});

