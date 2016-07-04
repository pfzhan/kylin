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

KylinApp.controller('UserSettingCtrl', function ($scope, $rootScope, $location, $base64, SweetAlert,AuthenticationService, UserService,KapUserService) {

  var user = UserService.getCurUser();
  $scope.confirmPassword = '';

  $scope.user ={
    username: '',
    password: '',
    newPassword: ''
  }

  AuthenticationService.login({}, {}, function (data) {
    $scope.loading = false;
    $scope.user.username = data.userDetails.username;
  }, function (error) {
  });


  $scope.resetPassword = function(){

    if($scope.confirmPassword !== $scope.user.newPassword){
      SweetAlert.swal('', "New password and confirm password is not the same.", 'error');
      return;
    }

    KapUserService.reset({},$scope.user,function(){
      SweetAlert.swal('', 'Reset password successfully.', 'success');
    },function(e){
      var message = "Failed to reset password."
      if( e.data&& e.data.exception){
        message = e.data.exception;
      }
      SweetAlert.swal('', message, 'error');
    })
  }


});
