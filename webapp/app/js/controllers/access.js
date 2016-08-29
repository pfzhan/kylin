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

KylinApp.controller('AccessCtrl', function ($scope, $compile,AccessService, MessageService, AuthenticationService, SweetAlert,kylinCommon) {

  $scope.accessTooltip = $scope.dataKylin.project.tip_access;
  $scope.authorities = null;

  AuthenticationService.authorities({}, function (authorities) {
    $scope.authorities = authorities.stringList;
  });

  $scope.resetNewAcess = function () {
    $scope.newAccess = null;
  }

  $scope.renewAccess = function (entity) {
    $scope.newAccess = {
      uuid: entity.uuid,
      sid: null,
      principal: true,
      permission: 'READ'
    };
  }

  $scope.grant = function (type, entity, grantRequst) {
    var uuid = grantRequst.uuid;
    delete grantRequst.uuid;
    AccessService.grant({type: type, uuid: uuid}, grantRequst, function (accessEntities) {
      entity.accessEntities = accessEntities;
      $scope.resetNewAcess();
//            MessageService.sendMsg('Access granted!', 'success', {});
      kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_grant);
     // SweetAlert.swal('Success!', 'Access granted!', 'success');
    }, function (e) {
      if (e.status == 404) {
//                MessageService.sendMsg('User not found!', 'error', {});
        kylinCommon.error_alert($scope.dataKylin.alert.oops,$scope.dataKylin.alert.userNotFound);
        //SweetAlert.swal('Oops...', 'User not found!!', 'error');
      }
      else {
        if (e.data && e.data.exception) {
          var message = e.data.exception;
          var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
          kylinCommon.error_alert($scope.dataKylin.alert.oops,msg);
          //SweetAlert.swal('Oops...', msg, 'error');
        } else {
          kylinCommon.error_alert($scope.dataKylin.alert.oops,$scope.dataKylin.alert.error_info);
        }

      }
    });
  }

  $scope.update = function (type, entity, access, permission) {
    var updateRequst = {
      accessEntryId: access.id,
      permission: permission
    };
    AccessService.update({type: type, uuid: entity.uuid}, updateRequst, function (accessEntities) {
      entity.accessEntities = accessEntities;
//            MessageService.sendMsg('Access granted!', 'success', {});
      SweetAlert.swal('',$scope.dataKylin.alert.success_access, 'success');
    }, function (e) {
      kylinCommon.error_default(e);
    });

  }

  $scope.revoke = function (type, access, entity) {
    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_sure_to_revoke,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if(isConfirm){
      var revokeRequst = {
        type: type,
        uuid: entity.uuid,
        accessEntryId: access.id
      };
      AccessService.revoke(revokeRequst, function (accessEntities) {
        entity.accessEntities = accessEntities.accessEntryResponseList;
        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_access_been_revoked);
        //SweetAlert.swal('Success!', 'The access has been revoked.', 'success');
      }, function (e) {
        kylinCommon.error_alert($scope.dataKylin.alert.oops,$scope.dataKylin.alert.error_info);
      });
      }
    });

  }
});

