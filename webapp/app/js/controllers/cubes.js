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

KylinApp.controller('CubesCtrl', function ($scope, $q, $routeParams, $location, $modal, MessageService, CubeDescService,RawTablesService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, cubeConfig, ProjectModel, ModelService, MetaModel, CubeList,modelsManager,cubesManager,TableService,kylinCommon,language,VdmUtil,AdminStoreService) {

    $scope.cubeConfig = cubeConfig;
    $scope.cubeList = CubeList;
    $scope.modelsManager = modelsManager;
    //$scope.cubesManager = cubesManager;
    $scope.goToAddPage=function(){
      var proName=ProjectModel.getSelectedProject();
      if(VdmUtil.storage.getObject(proName)&&VdmUtil.storage.getObject(proName).name){
        if(VdmUtil.storage.get("tsCache")){
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
              $scope.cubeMetaFrame=VdmUtil.storage.getObject(proName);
              $scope.RawTables=VdmUtil.storage.getObject(proName+"_rawtable")||[];
            }else{
              VdmUtil.storage.remove(proName);
              VdmUtil.storage.remove(ProjectModel.getSelectedProject(proName+"_rawtable"));
            }
            $location.path("/cubes/add/");
            VdmUtil.storage.remove("tsCache");
          });
        }else{
          $scope.cubeMetaFrame=VdmUtil.storage.getObject(proName);
          $scope.RawTables=VdmUtil.storage.getObject(proName+"_rawtable")||[];
        }

      }else{
        $location.path("/cubes/add/");
      }
    }
    $scope.backUpCube=function(cubeName){
      loadingRequest.show();
      AdminStoreService.globalBackup({},{
        cube:cubeName
      },function(data){
        loadingRequest.hide();
        SweetAlert.swal({
          title: '',
          text: $scope.dataKylin.alert.tip_store_callback+" "+VdmUtil.linkArrObjectToString(data),
          type: 'success',
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        });
        setTimeout(function(){
          $('.showSweetAlert.visible').removeClass('visible');
        },1000);
      },function(){
        loadingRequest.hide();
      })
    }
    $scope.queryFilter= {
      "model":null
    };

    $scope.listParams = {
      cubeName: $routeParams.cubeName,
      projectName: $routeParams.projectName
    };
    if ($routeParams.projectName) {
      $scope.projectModel.setSelectedProject($routeParams.projectName);
    }
    CubeList.removeAll();
    $scope.loading = false;
    $scope.action = {};


    $scope.state = {
      filterAttr: 'create_time', filterReverse: true, reverseColumn: 'create_time',
      dimensionFilter: '', measureFilter: ''
    };

    $scope.refreshCube = function(cube){
      var queryParam = {
        cubeName: cube.name,
        projectName: $scope.projectModel.selectedProject
      };
      var defer = $q.defer();
      CubeService.list(queryParam, function(cubes){
        for(var index in cubes){
          if(cube.name === cubes[index].name){
            defer.resolve(cubes[index]);
            break;
          }
        }
        defer.resolve([]);
      },function(e){
        defer.resolve([]);
      })
      return defer.promise;
    }
    $scope.list = function (offset, limit) {
      var defer = $q.defer();
      if (!$scope.projectModel.projects.length) {
        defer.resolve([]);
        return defer.promise;
      }
      offset = (!!offset) ? offset : 0;
      limit = (!!limit) ? limit : 20;

      var queryParam = {offset: offset, limit: limit};
      if ($scope.listParams.cubeName) {
        queryParam.cubeName = $scope.listParams.cubeName;
      }
      queryParam.projectName = $scope.projectModel.selectedProject;
      queryParam.modelName = $scope.queryFilter.model;
      $scope.loading = true;
      return CubeList.list(queryParam).then(function (resp) {
          angular.forEach($scope.cubeList.cubes,function(cube,index){
            cube.streaming = false;
            CubeDescService.query({cube_name: cube.name}, {}, function (detail) {
              if (detail.length > 0 && detail[0].hasOwnProperty("name")) {
                cube.detail = detail[0];
                ModelService.list({projectName:$scope.projectModel.selectedProject,modelName:cube.detail.model_name}, function (_models) {
                  if(_models && _models.length){
                    for(var i=0;i<=_models.length;i++){
                      if(_models[i].name == cube.detail.model_name){
                        cube.model = _models[i];
                        var factTable = cube.model.fact_table;
                        TableService.get({tableName:factTable},function(table){
                          if(table && table.source_type == 1){
                            cube.streaming = true;
                          }
                        })
                        break;
                      }
                    }
                  }
                })
                defer.resolve(cube.detail);
              } else {
                SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_no_cube_detail, 'error');
              }
            }, function (e) {
              kylinCommon.error_default(e);
            });

          })
        $scope.loading = false;
        defer.resolve(resp);
        return defer.promise;

      },function(resp){
        $scope.loading = false;
        defer.resolve([]);
        SweetAlert.swal($scope.dataKylin.alert.oops, resp, 'error');
        return defer.promise;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if (newValue != oldValue || newValue == null) {
        CubeList.removeAll();
        $scope.reload();
      }
    });
    $scope.reload = function () {
      // trigger reload action in pagination directive
      $scope.action.reload = !$scope.action.reload;
    };

    $scope.loadDetail = function (cube) {
      var defer = $q.defer();
      if (cube.detail) {
        defer.resolve(cube.detail);
      } else {
        CubeDescService.query({cube_name: cube.name}, {}, function (detail) {
          if (detail.length > 0 && detail[0].hasOwnProperty("name")) {
            cube.detail = detail[0];
            cube.model = modelsManager.getModel(cube.detail.model_name);
              defer.resolve(cube.detail);

          } else {
            SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_no_cube_detail_loaded, 'error');
          }
        }, function (e) {
          kylinCommon.error_default(e);
        });
      }

      return defer.promise;
    };

    $scope.getTotalSize = function (cubes) {
      var size = 0;
      if (!cubes) {
        return 0;
      }
      else {
        for (var i = 0; i < cubes.length; i++) {
          size += cubes[i].size_kb;
        }
        return $scope.dataSize(size * 1024);
      }
    };

//    Cube Action
    $scope.enable = function (cube) {
      SweetAlert.swal({
        title: '',
        text: $scope.dataKylin.alert.tip_sure_to_enable_cube,
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
//                confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.enable({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
            kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_Enable_submitted);
            //enbalerawtable
            RawTablesService.enable({rawTableName:cube.name},{},function(){

            })
          },function(e){

            loadingRequest.hide();
            kylinCommon.error_default(e);
          });
        }
      });
    };

    $scope.purge = function (cube) {
      SweetAlert.swal({
        title: '',
        text:  $scope.dataKylin.alert.tip_to_purge_cube,
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.purge({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_purge_job);
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
          },function(e){
            loadingRequest.hide();
            kylinCommon.error_default(e);
          });
        }
      });
    }

    $scope.disable = function (cube) {

      SweetAlert.swal({
        title: '',
        text: $scope.dataKylin.alert.tip_disable_cube,
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          CubeService.disable({cubeId: cube.name}, {}, function (result) {
            loadingRequest.hide();
            $scope.refreshCube(cube).then(function(_cube){
              if(_cube && _cube.name){
                $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
              }
            });
            kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_disable_job_submit);
            //enbalerawtable
            RawTablesService.disable({rawTableName:cube.name},{},function(){

            })

          },function(e){
            loadingRequest.hide();
            kylinCommon.error_default(e);
          });


        }

      });
    };



    $scope.dropCube = function (cube) {

      SweetAlert.swal({
        title: '',
        text: $scope.dataKylin.alert.tip_metadata_cleaned_up,
        type: '',
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true
      }, function(isConfirm) {
        if(isConfirm){

          loadingRequest.show();
          //删除rawtable
          RawTablesService.delete({rawTableName:cube.name},{},function(){
            CubeService.drop({cubeId: cube.name}, {}, function (result) {
              loadingRequest.hide();
              kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.tip_cube_drop);
              //location.reload();
              $scope.cubeList.cubes.splice($scope.cubeList.cubes.indexOf(cube),1);

            },function(e){

              loadingRequest.hide();
              kylinCommon.error_default(e);
            });

          },function(e){
            loadingRequest.hide();
            kylinCommon.error_default(e);
          })




        }

      });
    };

    $scope.startJobSubmit = function (cube) {
      $scope.loadDetail(cube);
      // for streaming cube build tip
      if(cube.streaming){
        SweetAlert.swal({
          title: '',
          text: "Are you sure to start the build?",
          type: '',
          showCancelButton: true,
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "Yes",
          closeOnConfirm: true
        }, function(isConfirm) {
          if(isConfirm){
            loadingRequest.show();
            CubeService.rebuildStreamingCube(
              {
                cubeId: cube.name
              },
              {
                sourceOffsetStart:0,
                sourceOffsetEnd:'9223372036854775807',
                buildType:'BUILD'
              }, function (job) {
                loadingRequest.hide();
                SweetAlert.swal('Success!', 'Rebuild job was submitted successfully', 'success');
              },function(e){

                loadingRequest.hide();
                if(e.data&& e.data.exception){
                  var message =e.data.exception;
                  var msg = !!(message) ? message : 'Failed to take action.';
                  SweetAlert.swal('Oops...', msg, 'error');
                }else{
                  SweetAlert.swal('Oops...', "Failed to take action.", 'error');
                }
              });
          }
        })
        return;
      }

      $scope.metaModel={
        model:modelsManager.getModelByCube(cube.name)
      }
      if ($scope.metaModel.model.name) {
        if ($scope.metaModel.model.partition_desc.partition_date_column) {
          $modal.open({
            templateUrl: 'jobSubmit.html',
            controller: jobSubmitCtrl,
            resolve: {
              cube: function () {
                return cube;
              },
              scope:function(){
                return $scope;
              },
              metaModel:function(){
                return $scope.metaModel;
              },
              buildType: function () {
                return 'BUILD';
              }
            }
          });
        }
        else {

          SweetAlert.swal({
            title: '',
            text: $scope.dataKylin.alert.tip_to_start_build,
            type: '',
            closeOnConfirm: true,
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes"

          }, function(isConfirm) {
            if(isConfirm){

              loadingRequest.show();
              CubeService.rebuildCube(
                {
                  cubeId: cube.name
                },
                {
                  buildType: 'BUILD',
                  startTime: 0,
                  endTime: 0
                }, function (job) {

                  loadingRequest.hide();
                  kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_rebuild_job);
                },function(e){
                  loadingRequest.hide();
                  kylinCommon.error_default(e);
                });
            }

          });
        }
      }
    };

    $scope.startRefresh = function (cube) {
      $scope.metaModel={
        model:modelsManager.getModelByCube(cube.name)
      };
      $modal.open({
        templateUrl: 'jobRefresh.html',
        controller: jobSubmitCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          metaModel:function(){
            return $scope.metaModel;
          },
          scope:function(){
            return $scope;
          },
          buildType: function () {
            return 'REFRESH';
          }
        }
      });

    };

    $scope.cloneCube = function(cube){

      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_target_project, 'info');
        return;
      }

      $scope.loadDetail(cube).then(function () {
        $modal.open({
          templateUrl: 'cubeClone.html',
          controller: cubeCloneCtrl,
          windowClass:"clone-cube-window",
          resolve: {
            cube: function () {
              return cube;
            }
          }
        });
      });
    }


    $scope.cubeEdit = function (cube) {
      $location.path("cubes/edit/" + cube.name);
    }
    $scope.startMerge = function (cube) {
      $scope.metaModel={
        model:modelsManager.getModelByCube(cube.name)
      };
      $modal.open({
        templateUrl: 'jobMerge.html',
        controller: jobSubmitCtrl,
        resolve: {
          cube: function () {
            return cube;
          },
          metaModel:function(){
            return $scope.metaModel;
          },
          buildType: function () {
            return 'MERGE';
          },
          scope:function(){
            return $scope;
          },
        }
      });
    }
  });

var cubeCloneCtrl = function ($scope, $modalInstance, CubeService, MessageService, $location, cube, MetaModel, SweetAlert,ProjectModel, loadingRequest,language,kylinCommon) {

  $scope.projectModel = ProjectModel;

  $scope.dataKylin = language.getDataKylin();

  $scope.targetObj={
    cubeName:cube.descriptor+"_clone",
    targetProject:$scope.projectModel.selectedProject
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.cloneCube = function(){

    if(!$scope.targetObj.targetProject){
      SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_target_project, 'info');
      return;
    }

    $scope.cubeRequest = {
      cubeName:$scope.targetObj.cubeName,
      project:$scope.targetObj.targetProject
    }

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_clone_cube,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {

        loadingRequest.show();
        CubeService.clone({cubeId: cube.name}, $scope.cubeRequest, function (result) {
          loadingRequest.hide();
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_clone_cube);
          location.reload();
        }, function (e) {
          loadingRequest.hide();
          kylinCommon.error_default(e);
        });
      }
    });
  }

}


var jobSubmitCtrl = function ($scope,scope, $modalInstance, CubeService, MessageService, $location, cube, metaModel, buildType, SweetAlert, loadingRequest, CubeList,language,kylinCommon,$filter) {
  $scope.dataKylin = language.getDataKylin();
  $scope.cubeList = CubeList;
  $scope.cube = cube;
  $scope.metaModel = metaModel;
  $scope.jobBuildRequest = {
    buildType: buildType,
    startTime: 0,
    endTime: 0
  };
  $scope.message = "";
  var startTime;
  if(cube.segments.length == 0){
    startTime = (!!cube.detail.partition_date_start)?cube.detail.partition_date_start:0;
  }else{
    startTime = cube.segments[cube.segments.length-1].date_range_end;
  }
  $scope.jobBuildRequest.startTime=startTime;
  $scope.rebuild = function () {

    $scope.message = null;

    if ($scope.jobBuildRequest.startTime >= $scope.jobBuildRequest.endTime) {
      $scope.message = "WARNING: End time should be later than the start time.";
      return;
    }

    loadingRequest.show();
    CubeService.rebuildCube({cubeId: cube.name}, $scope.jobBuildRequest, function (job) {
      loadingRequest.hide();
      $modalInstance.dismiss('cancel');
      SweetAlert.swal($scope.dataKylin.alert.success, $scope.dataKylin.alert.success_rebuild_job, 'success');
      scope.refreshCube(cube).then(function(_cube){
        $scope.cubeList.cubes[$scope.cubeList.cubes.indexOf(cube)] = _cube;
      });
    }, function (e) {


      loadingRequest.hide();
      if (e.data && e.data.exception) {
        var message = e.data.exception;

        if(message.indexOf("Empty cube segment found")!=-1){
          var _segment = message.substring(message.indexOf(":")+1,message.length-1);
          SweetAlert.swal({
            title:'',
            type:'info',
            text: $scope.dataKylin.alert.tip_rebuild_part_one+':'+_segment+$scope.dataKylin.alert.tip_rebuild_part_two,
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            closeOnConfirm: true
          }, function (isConfirm) {
            if (isConfirm) {
              $scope.jobBuildRequest.forceMergeEmptySegment = true;
              $scope.rebuild();
            }
          });
          return;
        }

        var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
        SweetAlert.swal($scope.dataKylin.alert.oops, msg, 'error');
      } else {
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.error_info, 'error');
      }
    });
  };

  // used by cube segment refresh
  $scope.segmentSelected = function (selectedSegment) {
    $scope.jobBuildRequest.startTime = 0;
    $scope.jobBuildRequest.endTime = 0;

    if (selectedSegment.date_range_start) {
      $scope.jobBuildRequest.startTime = selectedSegment.date_range_start;
    }

    if (selectedSegment.date_range_end) {
      $scope.jobBuildRequest.endTime = selectedSegment.date_range_end;
    }
  };

  // used by cube segments merge
  $scope.mergeStartSelected = function (mergeStartSeg) {
    $scope.jobBuildRequest.startTime = 0;

    if (mergeStartSeg.date_range_start) {
      $scope.jobBuildRequest.startTime = mergeStartSeg.date_range_start;
    }
  };

  $scope.mergeEndSelected = function (mergeEndSeg) {
    $scope.jobBuildRequest.endTime = 0;

    if (mergeEndSeg.date_range_end) {
      $scope.jobBuildRequest.endTime = mergeEndSeg.date_range_end;
    }
  };

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };


};


var streamingBuildCtrl = function ($scope, $modalInstance,kylinConfig,kylinCommon) {
  $scope.kylinConfig = kylinConfig;
  var streamingGuildeUrl = kylinConfig.getProperty("kylin.web.streaming.guide");
  $scope.streamingBuildUrl = streamingGuildeUrl?streamingGuildeUrl:"http://kylin.apache.org/";

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };
}
