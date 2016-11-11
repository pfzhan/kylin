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

KylinApp.controller('LoginCtrl', function ($scope, $rootScope, $location, $base64, AuthenticationService, UserService,ProjectService,ProjectModel) {
  $scope.username = null;
  $scope.password = null;
  $scope.loading = false;

  $scope.login = function () {
    $rootScope.userAction.islogout = false;
    // set the basic authentication header that will be parsed in the next request and used to authenticate
    httpHeaders.common['Authorization'] = 'Basic ' + $base64.encode($scope.username + ':' + $scope.password);
    $scope.loading = true;

    AuthenticationService.login({}, {}, function (data) {
      $scope.loading = false;
      $rootScope.$broadcast('event:loginConfirmed');
      UserService.setCurUser(data);
      $location.path(UserService.getHomePage());
    }, function (error) {
      $scope.loading = false;
      $scope.error = "Unable to login, please check your username/password.";
    });
  };
});
