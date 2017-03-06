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

KylinApp.controller('AdminCtrl', function ($scope, AdminService, CacheService, TableService, loadingRequest, MessageService, $modal, SweetAlert,kylinConfig,ProjectModel,$window,kylinCommon,AdminStoreService,VdmUtil) {
  $scope.configStr = "";
  $scope.envStr = "";

  $scope.isCacheEnabled = function(){
    return kylinConfig.isCacheEnabled();
  }

  $scope.getEnv = function () {
    AdminService.env({}, function (env) {
      $scope.envStr = env.env;
      MessageService.sendMsg($scope.dataKylin.alert.success_server_environment, 'success', {}, 3);

    }, function (e) {
      kylinCommon.error_default(e);
    });
  }

  $scope.getConfig = function () {
    AdminService.config({}, function (config) {
      $scope.configStr = config.config;
      MessageService.sendMsg($scope.dataKylin.alert.success_server_config, 'success', {}, 3);
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
        AdminService.updateConfig({}, {key: 'kylin.query.cache-enabled', value: false}, function () {
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
        AdminService.updateConfig({}, {key: 'kylin.query.cache-enabled', value: true}, function () {
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
  var actionDiagnosisCtrl = function ($scope, $modalInstance, KybotDiagnosisService, MessageService,language,kylinCommon){
    $scope.dataKylin = language.getDataKylin();
    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };
    $scope.upload=function(){
      $scope.loading=true;
      KybotDiagnosisService.uploadPackage(function(data){
        $scope.cancel();
        if(data&&data.code=='000'){
          SweetAlert.swal({
            title:$scope.dataKylin.alert.complete,
            text:$scope.dataKylin.alert.goToKybot,
            type: 'success',
            showCancelButton:'true',
            confirmButtonColor: '#DD6B55',
            confirmButtonText:$scope.dataKylin.alert.goToKybotBtn,
            cancelButtonText:$scope.dataKylin.alert.close,
            closeOnConfirm: true
          }, function (isConfirm) {
            if(isConfirm){
              $window.open('http://www.kybot.io');
            }
          })
        }
        $scope.loading=false;
      },function(e){
        var errorMsg='';
        if(e&&e.data){
          if(e.status==400) {
            if (e.data.code == '402') {
              errorMsg = 'Authentication failed. Please check your network connection availability to https://kybot.io';
            } else if (e.data.code == '401') {
              errorMsg = 'Please set kap.kyaccount.username and kap.kyaccount.password in kylin.properties.'
            }
          }else{
            errorMsg=e.data.exception||'';
          }
        }

        $scope.loading=false;
        $scope.cancel();
        SweetAlert.swal({
          title:$scope.dataKylin.alert.failUpload,// '上传失败',
          text:errorMsg||$scope.dataKylin.alert.error_info,
          type: 'error',
          confirmButtonColor: '#DD6B55',
          confirmButtonText: "ok",
          closeOnConfirm: true
        }, function (isConfirm) {})
      })
    }
    $scope.dowloadLink=function(){
       $scope.loading=true;
       VdmUtil.loadFileFromService(Config.service.url+"kybot/dump",function(){
         $scope.cancel();
       },function(e){
         $scope.cancel();
         SweetAlert.swal({
           title:$scope.dataKylin.alert.failDownload,
           text:e&&e.exception||$scope.dataKylin.alert.error_info,
           type: 'error',
           confirmButtonColor: '#DD6B55',
           confirmButtonText: "ok",
           closeOnConfirm: true
         }, function (isConfirm) {})
       })
    }

  }
  $scope.downloadBadQueryFiles = function(){
    //var _project = ProjectModel.selectedProject;
    //if (_project == null){
    //  SweetAlert.swal('', $scope.dataKylin.alert.tip_no_project_selected, 'info');
    //  return;
    //}
    $modal.open({
      templateUrl: 'actionDiagnosis.html',
      controller: actionDiagnosisCtrl,
      resolve: {},
      windowClass:'kybot_dialog'
    });
    //var  _project="-all";
    //var downloadUrl = Config.service.url + 'diag/project/'+_project+'/download';
    //$window.open(downloadUrl);


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
