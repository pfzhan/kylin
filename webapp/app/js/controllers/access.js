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

