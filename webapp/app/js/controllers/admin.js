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

KylinApp.controller('AdminCtrl', function ($scope, AdminService, CacheService, TableService, loadingRequest, MessageService, $modal, SweetAlert,kylinConfig,ProjectModel,$window,kylinCommon,AdminStoreService,VdmUtil) {
  $scope.configStr = "";
  $scope.envStr = "";

  $scope.isCacheEnabled = function(){
    console.log("cache enabled?:"+kylinConfig.isCacheEnabled());
    return kylinConfig.isCacheEnabled();
  }

  $scope.getEnv = function () {
    AdminService.env({}, function (env) {
      $scope.envStr = env.env;
      MessageService.sendMsg($scope.dataKylin.alert.success_server_environment, 'success', {});

    }, function (e) {
      kylinCommon.error_default(e);
    });
  }

  $scope.getConfig = function () {
    AdminService.config({}, function (config) {
      $scope.configStr = config.config;
      MessageService.sendMsg($scope.dataKylin.alert.success_server_config, 'success', {});
    }, function (e) {
      kylinCommon.error_default(e);
    });
  }

  $scope.reloadMeta = function () {
    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_reload_metadata,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        CacheService.clean({}, function () {
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_cache_reload);
        }, function (e) {
          kylinCommon.error_default(e);
        });
      }

    });
  }

  $scope.calCardinality = function (tableName) {
    $modal.open({
      templateUrl: 'calCardinality.html',
      controller: CardinalityGenCtrl,
      resolve: {
        tableName: function () {
          return tableName;
        },
        scope: function () {
          return $scope;
        }
      }
    });
  }

  $scope.cleanStorage = function () {
    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_clean_HDFS_HBase,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        AdminService.cleanStorage({}, function () {
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_storage_cleaned);
        }, function (e) {
          kylinCommon.error_default(e);
        });
      }
    });
  }

  $scope.disableCache = function () {
    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_disable_cache,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        AdminService.updateConfig({}, {key: 'kylin.query.cache.enabled', value: false}, function () {
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_cache_disabled);
          location.reload();
        }, function (e) {
          kylinCommon.error_default(e);
        });
      }

    });

  }

  $scope.enableCache = function () {
    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_enable_query_cache,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        AdminService.updateConfig({}, {key: 'kylin.query.cache.enabled', value: true}, function () {
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_cache_enabled);
          location.reload();
        }, function (e) {
          kylinCommon.error_default(e);
        });
      }

    });

  }

  $scope.toSetConfig = function () {
    $modal.open({
      templateUrl: 'updateConfig.html',
      controller: updateConfigCtrl,
      resolve: {}
    });
  }

  var CardinalityGenCtrl = function ($scope, $modalInstance, tableName, MessageService,language,kylinCommon) {
    $scope.dataKylin = language.getDataKylin();
    $scope.tableName = tableName;
    $scope.delimiter = 0;
    $scope.format = 0;
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.calculate = function () {
      $modalInstance.dismiss();
      loadingRequest.show();
      TableService.genCardinality({tableName: $scope.tableName}, {
        delimiter: $scope.delimiter,
        format: $scope.format
      }, function (result) {
        loadingRequest.hide();
        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_cardinality_job);
      }, function (e) {
        loadingRequest.hide();
        kylinCommon.error_default(e);
      });
    }
  };

  var updateConfigCtrl = function ($scope, $modalInstance, AdminService, MessageService,language,kylinCommon) {
    $scope.dataKylin = language.getDataKylin();
    $scope.state = {
      key: null,
      value: null
    };
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.update = function () {


      AdminService.updateConfig({}, {key: $scope.state.key, value: $scope.state.value}, function (result) {
        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_config_updated);
        $modalInstance.dismiss();
      }, function (e) {
        kylinCommon.error_default(e);
      });
    }
  };

  $scope.downloadBadQueryFiles = function(){
    //var _project = ProjectModel.selectedProject;
    //if (_project == null){
    //  SweetAlert.swal('', $scope.dataKylin.alert.tip_no_project_selected, 'info');
    //  return;
    //}
    var  _project="-all";
    var downloadUrl = Config.service.url + 'diag/project/'+_project+'/download';
    $window.open(downloadUrl);
  }

  $scope.backupGlobal=function(){
    loadingRequest.show();
    AdminStoreService.globalBackup({},{},function(data){
      loadingRequest.hide();
      SweetAlert.swal({
        title: '',
        text: $scope.dataKylin.alert.tip_store_callback+" "+VdmUtil.linkArrObjectToString(data),
        type: 'success',
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        closeOnConfirm: true,
      });
      setTimeout(function(){
        $('.showSweetAlert.visible').removeClass('visible');
      },1000);
    },function(e){
      kylinCommon.error_default(e);
      loadingRequest.hide();
    })
  }
  $scope.getEnv();
  $scope.getConfig();

});
