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
