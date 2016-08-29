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

KylinApp.controller('UserSettingCtrl', function ($scope, $rootScope, $location, $base64, SweetAlert,AuthenticationService, UserService,KapUserService,kylinCommon) {

  var user = UserService.getCurUser();
  $scope.confirmPassword = '';

  $scope.error;

  $scope.user ={
    username: '',
    password: '',
    newPassword: ''
  }

  $scope.role;

  $scope.init =  function(){
    if(UserService.hasRole('ROLE_ADMIN')){
      $scope.role = "ADMIN";
    }
    else if(UserService.hasRole('ROLE_MODELER')){
      $scope.role = "MODELER";
    }
    else if(UserService.hasRole('ROLE_ANALYST')){
      $scope.role = "ANALYST";
    }
  }();

  AuthenticationService.login({}, {}, function (data) {
    $scope.loading = false;
    $scope.user.username = data.userDetails.username;
  }, function (error) {
  });


  $scope.resetPassword = function(){

    $scope.error ='';
    if($scope.user.newPassword != $scope.confirmPassword){
      $scope.error = $scope.dataKylin.user.tip_error_not_same;
      return;
    }
    if(!UserService.hasRole('ROLE_ADMIN')&&$scope.user.password==''){
      $scope.error = $scope.dataKylin.user.tip_password_invalid;
      return;
    }

    KapUserService.reset({},$scope.user,function(){
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_resetSuccess);
    },function(e){
      var message = $scope.dataKylin.user.failed_reset;
      if( e.data&& e.data.exception){
        message = e.data.exception;
      }
      SweetAlert.swal('', message, 'error');
    })
  }


});
