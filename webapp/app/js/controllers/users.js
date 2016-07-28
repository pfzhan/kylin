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

KylinApp.controller('UsersCtrl', function ($scope, $rootScope, $location, $base64, SweetAlert,AuthenticationService, UserService,KapUserService,tableConfig,$modal,language,kylinCommon) {

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

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.user.alert_sure_to_drop,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        KapUserService.dropUser({id: user.username}, {}, function () {
          var _index = findUserIndex($scope.users, user);
          //var _index =  _.findIndex($scope.users,function(item){return item.username == user.username})
          $scope.users.splice(_index, 1);
          kylinCommon.success_alert($scope.dataKylin.user.success_drop);
        }, function () {
          SweetAlert.swal('', message, 'error');
        })
      }
      else {
      }
    })
  }

  $scope.disableUser = function(user, index){
    KapUserService.update({id:user.username},{'username':user.username,'disabled':'true'},function(user){
      var _index = findUserIndex($scope.users, user);
      //var _index =  _.findIndex($scope.users,function(item){return item.username == user.username});
      $scope.users[_index] = userDetailToUser(user);
      kylinCommon.success_alert($scope.dataKylin.user.success_disable);
    },function(){
      SweetAlert.swal('', message, 'error');
    })

  }

  $scope.enableUser = function(user, index){
    KapUserService.update({id:user.username},{'username':user.username,'disabled':'false'},function(user){
      var _index = findUserIndex($scope.users, user);
      //var _index =  _.findIndex($scope.users,function(item){return item.username == user.username});
      $scope.users[_index] = userDetailToUser(user);
      kylinCommon.success_alert($scope.dataKylin.user.success_enable);
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
    var message = $scope.dataKylin.user.tip_failed_list_users;
    if( e.data&& e.data.exception){
      message = e.data.exception;
    }
    SweetAlert.swal('', message, 'error');
  })

});


var addUserCtrl = function ($scope, $modalInstance, SweetAlert, KapUserService, scope, loadingRequest,language,kylinCommon) {
  $scope.dataKylin=language.getDataKylin();
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
      $scope.error = $scope.dataKylin.user.tip_error_not_same;
      return;
    }
    if($scope.newUser.username==''){
      $scope.error = $scope.dataKylin.user.tip_username_invalid;
      return;
    }
    if($scope.newUser.password==''){
      $scope.error = $scope.dataKylin.user.tip_password_invalid;
      return;
    }

    var userDetail = userToUserDetail($scope.newUser);
    KapUserService.save({id:userDetail.username},userDetail,function(user){
      $modalInstance.dismiss('cancel');
      scope.users.push(userDetailToUser(user));
      kylinCommon.success_alert($scope.dataKylin.user.success_add_user);
    },function(){
      SweetAlert.swal('', message, 'error');
    })

  }

}

var editUserCtrl = function ($scope, $modalInstance, SweetAlert, KapUserService, scope, user,language,kylinCommon) {
  $scope.dataKylin=language.getDataKylin();
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
      kylinCommon.success_alert($scope.dataKylin.user.success_update_user);
    },function(){
      SweetAlert.swal('', message, 'error');
    })

  }

}

var resetUserCtrl = function ($scope, $modalInstance, SweetAlert, KapUserService, scope, user,language,kylinCommon) {
  $scope.dataKylin=language.getDataKylin();
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
      $scope.error = $scope.dataKylin.user.tip_error_not_same;
      return;
    }

    var userDetail = userToUserDetail($scope.resetUser);
    KapUserService.reset({},userDetail,function(user){
      $modalInstance.dismiss('cancel');
      kylinCommon.success_alert($scope.dataKylin.user.success_resetSuccess);
    },function(e){
      var message = $scope.dataKylin.user.failed_reset;
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

