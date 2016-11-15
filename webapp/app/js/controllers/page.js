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

KylinApp.controller('PageCtrl', function ($scope, $q, AccessService, $modal, $location, $rootScope, $routeParams, $http, UserService, ProjectService, SweetAlert, $cookieStore, language,$log, kylinConfig, ProjectModel, TableModel,kapEnglishConfig,kapChineseConfig,kylinCommon) {

  //init kylinConfig to get kylin.Propeties
  kylinConfig.init().$promise.then(function (data) {
    kylinConfig.initWebConfigInfo();
  });

  $scope.language = language;
  $rootScope.userAction={
   'islogout':false
  }
  $scope.requestLink='api/kap/system/requestLicense';
  $scope.dataEnglish = kapEnglishConfig;

  $scope.dataChina = kapChineseConfig;

  $scope.languageType = $cookieStore.get('language')?$cookieStore.get('language'):0;

  $scope.dataKylin = {};
  $scope.dataInit = function () {
    $scope.dataKylin = $scope.languageType == 0?$scope.dataEnglish:$scope.dataChina;
    language.setLanguageType($scope.languageType);
    language.setDataKylin($scope.dataKylin);
  };

  $scope.dataInit();

  $scope.$watch('languageType', function (newValue, oldValue) {
    if (newValue != oldValue) {
      $scope.languageType = newValue;
      $cookieStore.put('language',$scope.languageType);
      $scope.dataInit();
      $scope.$broadcast("finish",$scope.languageType);

    }

  });
  $scope.kylinConfig = kylinConfig;

  $scope.header = {show: true};

  $scope.$on('$routeChangeSuccess', function ($event, current) {
    $scope.activeTab = current.tab;
    $scope.header.show = ($location.url() && $location.url().indexOf('/home') == -1);
  });

  $scope.config = Config;
  $scope.routeParams = $routeParams;
  $scope.angular = angular;
  $scope.userService = UserService;
  $scope.activeTab = "";
  $scope.projectModel = ProjectModel;
  $scope.tableModel = TableModel;


  // Set up common methods
  $scope.logout = function () {
    ProjectModel.clear();
    $rootScope.userAction.islogout = true;
    $scope.$emit('event:logoutRequest');
    $http.get(Config.service.base + 'j_spring_security_logout').success(function () {
      UserService.setCurUser({});
      $scope.username = $scope.password = null;
      $location.path('/login');
    }).error(function () {
      UserService.setCurUser({});
      $scope.username = $scope.password = null;
      $location.path('/login');
    });
    ;
  };

  $scope.aboutKap = function(){
    $modal.open({
      templateUrl: 'aboutKap.html',
      controller: aboutKapCtrl,
      windowClass:"about-kap-window",
      resolve: {
      }
    });
  }


  Messenger.options = {
    extraClasses: 'messenger-fixed messenger-on-bottom messenger-on-right',
    theme: 'air'
  };

  $scope.getInt = function (ivalue) {
    return parseInt(ivalue);
  };

  $scope.getLength = function (obj) {
    if (!obj) {
      return 0;
    }
    var size = 0, key;
    for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
    }
    return size;
  };

  // common acl methods
  $scope.hasPermission = function (entity) {
    var curUser = UserService.getCurUser();
    if (!curUser.userDetails) {
      return curUser;
    }

    var hasPermission = false;
    var masks = [];
    for (var i = 1; i < arguments.length; i++) {
      if (arguments[i]) {
        masks.push(arguments[i]);
      }
    }

    if (entity) {
      angular.forEach(entity.accessEntities, function (acessEntity, index) {
        if (masks.indexOf(acessEntity.permission.mask) != -1) {
          if ((curUser.userDetails.username == acessEntity.sid.principal) || UserService.hasRole(acessEntity.sid.grantedAuthority)) {
            hasPermission = true;
          }
        }
      });
    }

    return hasPermission;
  };

  $scope.listAccess = function (entity, type) {
    var defer = $q.defer();

    entity.accessLoading = true;
    AccessService.list({type: type, uuid: entity.uuid}, function (accessEntities) {
      entity.accessLoading = false;
      entity.accessEntities = accessEntities;
      defer.resolve();
    });

    return defer.promise;
  };

  // Compute data size so as to auto convert to KB/MB/GB/TB)
  $scope.dataSize = function (data) {
    var size;
    if (data / 1024 / 1024 / 1024 / 1024 >= 1) {
      size = (data / 1024 / 1024 / 1024 / 1024).toFixed(2) + ' TB';
    } else if (data / 1024 / 1024 / 1024 >= 1) {
      size = (data / 1024 / 1024 / 1024).toFixed(2) + ' GB';
    } else if (data / 1024 / 1024 >= 1) {
      size = (data / 1024 / 1024).toFixed(2) + ' MB';
    } else {
      size = (data / 1024).toFixed(2) + ' KB';
    }
    return size;
  };


  $scope.toCreateProj = function () {
    $modal.open({
      templateUrl: 'project.html',
      controller: projCtrl,
      resolve: {
        projects: function () {
          return null;
        },
        project: function () {
          return null;
        }
      }
    });
  };




  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if (newValue != oldValue) {
      if(!$rootScope.userAction.islogout) {
        //$log.log("project updated in page controller,from:"+oldValue+" To:"+newValue);
        $cookieStore.put("project", $scope.projectModel.selectedProject);
      }
    }

  });

  /*
   *global method for all scope to use
   * */

  //update scope value to view
  $scope.safeApply = function (fn) {
    var phase = this.$root.$$phase;
    if (phase == '$apply' || phase == '$digest') {
      if (fn && (typeof(fn) === 'function')) {
        fn();
      }
    } else {
      this.$apply(fn);
    }
  };

});

var aboutKapCtrl = function($scope,KapSystemService,language,$modalInstance){
  $scope.dataKylin = language.getDataKylin();
  $scope.license = {};
  KapSystemService.license({},function(data){
    if(!data['kap.version']){
      data['kap.version'] = 'N/A';
    }
    if(!data['kap.license.statement']){
      data['kap.license.statement'] = 'N/A';
    }
    if(!data['kap.dates']){
      data['kap.dates'] = 'N/A';
    }
    if(!data['kap.commit']){
      data['kap.commit'] = 'N/A';
    }
    if(!data['kylin.commit']){
      data['kylin.commit'] = 'N/A';
    }
    if(!data['kap.license.isEvaluation']){
      data['kap.license.isEvaluation'] = "false";
    }
    if(!data['kap.license.serviceEnd']){
      data['kap.license.serviceEnd'] = "N/A";
    }
    if(!data['kylin.commit']){
      data['kylin.commit'] = 'N/A';
    }


    $scope.license = data;
  },function(){

  })

  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

}

var projCtrl = function ($scope, $location, $modalInstance, ProjectService, MessageService, projects, project, SweetAlert, ProjectModel, $cookieStore, $route,language,kylinCommon) {
  $scope.state = {
    isEdit: false,
    oldProjName: null,
    projectIdx: -1
  };

  $scope.isEdit = false;
  $scope.proj = {name: '', description: ''};
  $scope.dataKylin = language.getDataKylin();
  if (project) {
    $scope.state.isEdit = true;
    $scope.state.oldProjName = project.name;
    $scope.proj = project;
    for (var i = 0; i < projects.length; i++) {
      if (projects[i].name === $scope.state.oldProjName) {
        $scope.state.projectIdx = i;
        break;
      }
    }
  }

  $scope.createOrUpdate = function () {
    if ($scope.state.isEdit) {

      var requestBody = {
        formerProjectName: $scope.state.oldProjName,
        newProjectName: $scope.proj.name,
        newDescription: $scope.proj.description
      };
      ProjectService.update({}, requestBody, function (newProj) {
        kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_project_update);
        //update project in project model
        ProjectModel.updateProject($scope.proj.name, $scope.state.oldProjName);
        $cookieStore.put("project", $scope.proj.name);
        ProjectModel.setSelectedProject($scope.proj.name);
        $modalInstance.dismiss('cancel');
      }, function (e) {
        kylinCommon.error_default(e);
      });
    }
    else {
      ProjectService.save({}, $scope.proj, function (newProj) {
        SweetAlert.swal($scope.dataKylin.alert.success, $scope.dataKylin.alert.tip_new_project_created, 'success');
        $modalInstance.dismiss('cancel');
//                if(projects) {
//                    projects.push(newProj);
//                }
//                ProjectModel.addProject(newProj);
        $cookieStore.put("project", newProj.name);
        location.reload();
      }, function (e) {
        kylinCommon.error_default(e);
      });
    }
  };

  $scope.cancel = function () {
    if ($scope.state.isEdit) {
      projects[$scope.state.projectIdx].name = $scope.state.oldProjName;
    }
    $modalInstance.dismiss('cancel');
  };

};


