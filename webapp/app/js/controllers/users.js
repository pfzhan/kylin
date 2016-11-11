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

KylinApp.controller('UsersCtrl', function ($scope, $rootScope, $location, $base64, SweetAlert,AuthenticationService, UserService,KapUserService,tableConfig,$modal,language,kylinCommon,uiGridConstants) {


  $scope.girdData = [];

  $scope.init = function () {

    $scope.griddisable=$scope.dataKylin.user.disable;
    $scope.gridenable=$scope.dataKylin.user.enable;
    $scope.editRole=$scope.dataKylin.user.editRole;
    $scope.resetPassword=$scope.dataKylin.user.resetPassword;
    $scope.drop=$scope.dataKylin.user.drop;
    $scope.items_per_page=$scope.dataKylin.user.items_per_page;


    $scope.gridOptions = {
      data:'girdData',
      columnDefs: [
        {
          field: 'username',
          displayName: $scope.dataKylin.user.usertheaditems[0].name,
          sort: {
            direction: uiGridConstants.ASC,
          },
          enableColumnMenu: false,
          enableCellEdit: false,
        },
        {
          field: 'admin',
          displayName: $scope.dataKylin.user.usertheaditems[1].name,
          enableFiltering: false,
          enableColumnMenu: false,
          cellTemplate: '<i class="fa fa-check" ng-if="grid.getCellValue(row, col)" style="margin-left: 2%;"></i>'
        },
        {
          field: 'modeler',
          displayName: $scope.dataKylin.user.usertheaditems[2].name,
          enableFiltering: false,
          enableColumnMenu: false,
          cellTemplate: '<i class="fa fa-check" ng-if="grid.getCellValue(row, col)" style="margin-left: 2%;"></i>'
        },
        {
          field: 'analyst',
          displayName: $scope.dataKylin.user.usertheaditems[3].name,
          enableFiltering: false,
          enableColumnMenu: false,
          cellTemplate: '<i class="fa fa-check" ng-if="grid.getCellValue(row, col)" style="margin-left: 2%;"></i>'
        },
        {
          field: 'disabled',
          displayName: $scope.dataKylin.user.usertheaditems[4].name,
          enableFiltering: false,
          enableColumnMenu: false,
          cellTemplate: '<span class="label badge" ng-class="{\'label-success\': grid.getCellValue(row, col)==false, \'label-default\': grid.getCellValue(row, col)==true}" style="margin-left: 2%;margin-top: 2%;">{{grid.getCellValue(row, col)?grid.appScope.griddisable:grid.appScope.gridenable}}</span>'
        },
        {
          field: 'disabled',
          displayName: "Action",
          enableFiltering: false,
          enableSorting: false,
          enableColumnMenu: false,
          cellClass:'users-paging-grid',
          cellTemplate: '<div ng-click="$event.stopPropagation();" class="btn-group"  style="margin-left: 2%;padding-top: 1%;"><button type="button" class="btn btn-default btn-xs dropdown-toggle" data-toggle="dropdown">Action <span class="ace-icon fa fa-caret-down icon-on-right"></span></button><ul class="dropdown-menu" role="menu" ><li><a ng-click="grid.appScope.editUser(row)">{{grid.appScope.editRole}}</a></li> <li><a ng-click="grid.appScope.resetPassowrd(row)">{{grid.appScope.resetPassword}}</a></li> <li><a ng-click="grid.appScope.dropUser(row)">{{grid.appScope.drop}}</a></li><li><a ng-click="grid.appScope.enableUser(row)" ng-if="grid.getCellValue(row, col)">{{grid.appScope.gridenable}}</a></li> <li><a ng-click="grid.appScope.disableUser(row)" ng-if="!grid.getCellValue(row, col)">{{grid.appScope.griddisable}}</a></li> </ul> </div>'
        }
      ],
      enableFiltering: true,
      enableSorting: true,
      enableHorizontalScrollbar: 0,
      enableVerticalScrollbar: 0,
      enablePagination: true,
      paginationCurrentPage: 1,
      paginationPageSize: 15,
      paginationTemplate:"<div role=\"contentinfo\" class=\"ui-grid-pager-panel\" ui-grid-pager ng-show=\"grid.options.enablePaginationControls\" ><div role=\"navigation\" class=\"ui-grid-pager-container\"><div role=\"menubar\" class=\"ui-grid-pager-control\"><button type=\"button\" role=\"menuitem\" class=\"ui-grid-pager-first\" ui-grid-one-bind-title=\"aria.pageToFirst\" ui-grid-one-bind-aria-label=\"aria.pageToFirst\" ng-click=\"pageFirstPageClick()\" ng-disabled=\"cantPageBackward()\"><div ng-class=\"grid.isRTL() ? 'last-triangle' : 'first-triangle'\"><div ng-class=\"grid.isRTL() ? 'last-bar-rtl' : 'first-bar'\"></div></div></button> <button type=\"button\" role=\"menuitem\" class=\"ui-grid-pager-previous\" ui-grid-one-bind-title=\"aria.pageBack\" ui-grid-one-bind-aria-label=\"aria.pageBack\" ng-click=\"pagePreviousPageClick()\" ng-disabled=\"cantPageBackward()\"><div ng-class=\"grid.isRTL() ? 'last-triangle prev-triangle' : 'first-triangle prev-triangle'\"></div></button> <input type=\"number\" ui-grid-one-bind-title=\"aria.pageSelected\" ui-grid-one-bind-aria-label=\"aria.pageSelected\" class=\"ui-grid-pager-control-input\" ng-model=\"grid.options.paginationCurrentPage\" min=\"1\" max=\"{{ paginationApi.getTotalPages() }}\" required> <span class=\"ui-grid-pager-max-pages-number\" ng-show=\"paginationApi.getTotalPages() > 0\"><abbr ui-grid-one-bind-title=\"paginationOf\">/</abbr> {{ paginationApi.getTotalPages() }}</span> <button type=\"button\" role=\"menuitem\" class=\"ui-grid-pager-next\" ui-grid-one-bind-title=\"aria.pageForward\" ui-grid-one-bind-aria-label=\"aria.pageForward\" ng-click=\"pageNextPageClick()\" ng-disabled=\"cantPageForward()\"><div ng-class=\"grid.isRTL() ? 'first-triangle next-triangle' : 'last-triangle next-triangle'\"></div></button> <button type=\"button\" role=\"menuitem\" class=\"ui-grid-pager-last\" ui-grid-one-bind-title=\"aria.pageToLast\" ui-grid-one-bind-aria-label=\"aria.pageToLast\" ng-click=\"pageLastPageClick()\" ng-disabled=\"cantPageToLast()\"><div ng-class=\"grid.isRTL() ? 'first-triangle' : 'last-triangle'\"><div ng-class=\"grid.isRTL() ? 'first-bar-rtl' : 'last-bar'\"></div></div></button></div><div class=\"ui-grid-pager-row-count-picker\" ng-if=\"grid.options.paginationPageSizes.length > 1\"></span></div><span ng-if=\"grid.options.paginationPageSizes.length <= 1\" class=\"ui-grid-pager-row-count-label\">{{grid.options.paginationPageSize}}&nbsp;{{grid.appScope.items_per_page}}</span></div><div class=\"ui-grid-pager-count-container\"><div class=\"ui-grid-pager-count\"><span ng-show=\"grid.options.totalItems > 0\">{{showingLow}} <abbr ui-grid-one-bind-title=\"paginationThrough\">-</abbr> {{showingHigh}} <abbr ui-grid-one-bind-title=\"paginationThrough\">/</abbr> {{grid.options.totalItems}} </span></div></div></div>", //自定义底部分页代码

      onRegisterApi: function(gridApi){
        $scope.gridApi = gridApi;
      },
    };

  }
  $scope.init();


  $scope.promise = KapUserService.listUsers({},function(users){
    angular.forEach(users,function(_user){
      $scope.girdData.push(userDetailToUser(_user));
    })
  },function(e){
    var message = $scope.dataKylin.user.tip_failed_list_users;
    SweetAlert.swal('', message, 'error');
  }).$promise;


  $scope.$on('finish', function(event,data) {
    $scope.init();
  });



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

  $scope.dropUser = function(user){
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
        KapUserService.dropUser({id: user.entity.username}, {}, function () {
          var _index = findUserIndex($scope.girdData, user.entity);
          //var _index =  _.findIndex($scope.girdData,function(item){return item.username == user.username})
          $scope.girdData.splice(_index, 1);
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_drop);
        }, function () {
          SweetAlert.swal('', message, 'error');
        })
      }
    })
  }

  $scope.disableUser = function(user){
    KapUserService.update({id:user.entity.username},{'username':user.username,'disabled':'true'},function(user){
      var _index = findUserIndex($scope.girdData, user);
      //var _index =  _.findIndex($scope.girdData,function(item){return item.username == user.username});
      $scope.girdData[_index] = userDetailToUser(user);
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_disable);
    },function(){
      SweetAlert.swal('', message, 'error');
    })
  }

  $scope.enableUser = function(user){
    KapUserService.update({id:user.entity.username},{'username':user.username,'disabled':'false'},function(user){
      var _index = findUserIndex($scope.girdData, user);
      //var _index =  _.findIndex($scope.girdData,function(item){return item.username == user.username});
      $scope.girdData[_index] = userDetailToUser(user);
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_enable);
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
          return user.entity;
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
          return user.entity;
        }
      }
    });
  }

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
    $scope.keepGoing = true;
    angular.forEach(scope.girdData, function(user){
      if($scope.keepGoing) {
        if($scope.newUser.username == user.username){
          $scope.error = $scope.dataKylin.user.tip_error_user_exits;
          $scope.keepGoing = false;
        }
      }
    });

    if(!$scope.keepGoing) return;
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
      scope.girdData.push(userDetailToUser(user));
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_add_user);
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
      var _index = findUserIndex(scope.girdData,user);
      scope.girdData[_index] = userDetailToUser(user);
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_update_user);
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
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.user.success_resetSuccess);
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

