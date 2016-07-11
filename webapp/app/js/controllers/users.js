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

KylinApp.controller('UsersCtrl', function ($scope, $rootScope, $location, $base64, SweetAlert,AuthenticationService, UserService,KapUserService,tableConfig,$modal) {

    $scope.tableConfig = tableConfig;
    $scope.users = [];
    $scope.newUser = function(){
      return {
        'username':'',
        'disabled':false,
        'admin':false,
        'modeler':false,
        'analyst':false
      }
    }

    $scope.state = {
      filterAttr: 'username', filterReverse: true, reverseColumn: 'username'
    };

    $scope.addUser = function(){
      $modal.open({
        templateUrl: 'addUser.html',
        controller: addUserCtrl,
        windowClass:"add-user-window",
        resolve: {
          scope: function(){
            return $scope
          }
        }
      });
    }

    $scope.dropUser = function(user, index){
      KapUserService.dropUser({id:user.username},{},function(){
        var _index = findUserIndex($scope.users, user);
        //var _index =  _.findIndex($scope.users,function(item){return item.username == user.username})
        $scope.users.splice(_index,1);
        SweetAlert.swal('Success!', 'Drop user successfully', 'success');
      },function(){
        SweetAlert.swal('', message, 'error');
      })
    }

    $scope.disableUser = function(user, index){
      KapUserService.update({id:user.username},{'username':user.username,'disabled':'true'},function(user){
        var _index = findUserIndex($scope.users, user);
        //var _index =  _.findIndex($scope.users,function(item){return item.username == user.username});
        $scope.users[_index] = userDetailToUser(user);
        SweetAlert.swal('Success!', 'Disable user successfully', 'success');
      },function(){
        SweetAlert.swal('', message, 'error');
      })

    }

    $scope.enableUser = function(user, index){
      KapUserService.update({id:user.username},{'username':user.username,'disabled':'false'},function(user){
        var _index = findUserIndex($scope.users, user);
        //var _index =  _.findIndex($scope.users,function(item){return item.username == user.username});
        $scope.users[_index] = userDetailToUser(user);
        SweetAlert.swal('Success!', 'Enable user successfully', 'success');
      },function(){
        SweetAlert.swal('', message, 'error');
      })

    }

    $scope.editUser = function(user){
      $modal.open({
        templateUrl: 'editUser.html',
        controller: editUserCtrl,
        windowClass:"add-user-window",
        resolve: {
          scope: function(){
            return $scope
          },
          user: function(){
            return user;
          }
        }
      });
    }

    $scope.resetPassowrd = function(user){
      $modal.open({
        templateUrl: 'resetUserpwd.html',
        controller: resetUserCtrl,
        windowClass:"add-user-window",
        resolve: {
          scope: function(){
            return $scope
          },
          user: function(){
            return user;
          }
        }
      });
    }

    KapUserService.listUsers({},$scope.user,function(users){
      angular.forEach(users,function(_user){
        $scope.users.push(userDetailToUser(_user));

      })

      //$scope.userGridOptions.data = $scope.users;

    },function(e){
      var message = "Failed to list users."
      if( e.data&& e.data.exception){
        message = e.data.exception;
      }
      SweetAlert.swal('', message, 'error');
    })

});


var addUserCtrl = function ($scope, $modalInstance, SweetAlert, KapUserService, scope, loadingRequest) {

  $scope.error='';
  $scope.confirmPassword = "";
  $scope.newUser = {
      'username':'',
      'password':'',
      'disabled':false,
      'admin':false,
      'modeler':false,
      'analyst':true,
      'confirmPassword':''
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.submitUser = function(){
    $scope.error ='';
    if($scope.newUser.password != $scope.newUser.confirmPassword){
      $scope.error = 'Password and confirm password are not the same.';
      return;
    }
    if($scope.newUser.username==''){
      $scope.error = 'Username invalid.';
      return;
    }
    if($scope.newUser.password==''){
      $scope.error = 'Password invalid.';
      return;
    }

    var userDetail = userToUserDetail($scope.newUser);
    KapUserService.save({id:userDetail.username},userDetail,function(user){
      $modalInstance.dismiss('cancel');
      scope.users.push(userDetailToUser(user));
      SweetAlert.swal('Success!', 'Add user successfully', 'success');
    },function(){
      SweetAlert.swal('', message, 'error');
    })

  }

}

var editUserCtrl = function ($scope, $modalInstance, SweetAlert, KapUserService, scope, user) {

  $scope.error='';
  $scope.confirmPassword = "";
  $scope.editUser = {
    'username':user.username,
    'password':user.password,
    'disabled':user.disabled,
    'admin':user.admin,
    'modeler':user.modeler,
    'analyst':user.analyst
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.submitUser = function(){

    var userDetail = userToUserDetail($scope.editUser);
    KapUserService.update({id:userDetail.username},userDetail,function(user){
      $modalInstance.dismiss('cancel');
      var _index = findUserIndex(scope.users,user);
      scope.users[_index] = userDetailToUser(user);
      SweetAlert.swal('Success!', 'Update user successfully', 'success');
    },function(){
      SweetAlert.swal('', message, 'error');
    })

  }

}

var resetUserCtrl = function ($scope, $modalInstance, SweetAlert, KapUserService, scope, user) {

  $scope.error='';
  $scope.resetUser = {
    'username':user.username,
    'password':user.password,
    'disabled':user.disabled,
    'admin':user.admin,
    'modeler':user.modeler,
    'analyst':user.analyst,
    'confirmPassword':''
  }

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.submitUser = function(){

    if($scope.resetUser.confirmPassword !== $scope.resetUser.newPassword){
      $scope.error ="New password and confirm password are not the same.";
      return;
    }

    var userDetail = userToUserDetail($scope.resetUser);
    KapUserService.reset({},userDetail,function(user){
      $modalInstance.dismiss('cancel');
      SweetAlert.swal('Success!', 'Reset user password successfully', 'success');
    },function(e){
      var message = "Failed to reset password."
      if( e.data&& e.data.exception){
        message = e.data.exception;
      }
      SweetAlert.swal('', message, 'error');
    })

  }

}


function userToUserDetail(user){

  var userDetail ={
    username:"",
    password:"",
    authorities:[],
    disabled:false
  }

  userDetail.username = user.username;
  userDetail.password = user.password;
  userDetail.newPassword = user.newPassword;
  if(user.admin){
    userDetail.authorities.push('ROLE_ADMIN');
  }
  if(user.modeler){
    userDetail.authorities.push('ROLE_MODELER');
  }
  if(user.analyst){
    userDetail.authorities.push('ROLE_ANALYST');
  }

  return userDetail;
}

function userDetailToUser(_user){
  var newUser = {
    'username':'',
    'disabled':false,
    'admin':false,
    'modeler':false,
    'analyst':false
  };
  newUser.username = _user.username;
  newUser.disabled = _user.disabled;

  for(var i = 0;i<_user.authorities.length;i++){
    var role = _user.authorities[i];
    switch(role.authority){
      case 'ROLE_ADMIN':
        newUser.admin = true;
      case 'ROLE_MODELER':
        newUser.modeler = true;
      case 'ROLE_ANALYST':
        newUser.analyst = true;
    }
  }
  return newUser;
}


function findUserIndex(users,user){
  var index;
  for(var i=0;i<users.length;i++){
    if(users[i].username==user.username){
      index = i;
    }
  }

  return index;
}
