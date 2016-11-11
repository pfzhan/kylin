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

KylinApp.run(function ($rootScope, $http, $location, UserService, AuthenticationService, MessageService, $cookieStore,ProjectService,ProjectModel,AccessService,SweetAlert,loadingRequest) {

  $rootScope.permissions = {
    READ: {name: 'CUBE QUERY', value: 'READ', mask: 1},
    MANAGEMENT: {name: 'CUBE EDIT', value: 'MANAGEMENT', mask: 32},
    OPERATION: {name: 'CUBE OPERATION', value: 'OPERATION', mask: 64},
    ADMINISTRATION: {name: 'CUBE ADMIN', value: 'ADMINISTRATION', mask: 16}
  };

  $rootScope.$on("$routeChangeStart", function () {
    AuthenticationService.ping(function (data) {
      UserService.setCurUser(data);
      if(!data.userDetails){
        $location.path(UserService.getHomePage());
      }else{
        //get project info when login
        if (!ProjectModel.projects.length&&!$rootScope.userAction.islogout) {

          loadingRequest.show();
          ProjectService.listReadable({}, function (projects) {
            loadingRequest.hide();

            if(!projects.length){
              return;
            }

            var _projects = [];
            _projects = _.sortBy(projects, function (i) {
              return i.name.toLowerCase();
            });
            ProjectModel.setProjects(_projects);
            var projectInCookie = $cookieStore.get("project");
            if(projectInCookie&&ProjectModel.getIndex(projectInCookie)==-1){
              projectInCookie = null;
            }
            var selectedProject = projectInCookie != null ? projectInCookie : null;
            if(projectInCookie!=null){
              selectedProject = projectInCookie;
            }else if(UserService.hasRole('ROLE_ADMIN')){
              selectedProject = null;
            }else{
              selectedProject = ProjectModel.projects[0].name
            }

            //var selectedProject = ProjectModel.selectedProject != null ? ProjectModel.selectedProject : projectInCookie != null ? projectInCookie : ProjectModel.projects[0].name;
            ProjectModel.setSelectedProject(selectedProject);
            angular.forEach(ProjectModel.projects, function (project, index) {
              project.accessLoading = true;
              AccessService.list({type: 'ProjectInstance', uuid: project.uuid}, function (accessEntities) {
                project.accessLoading = false;
                project.accessEntities = accessEntities;
              });
            });

          },function(e){
            loadingRequest.hide();
            $location.path(UserService.getHomePage());
          });
        }
      }
    });


    if ($location.url() == '' || $location.url() == '/') {
      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
        $location.path(UserService.getHomePage());
      });
      return;
    }
  });

  /**
   * Holds all the requests which failed due to 401 response.
   */
  $rootScope.requests401 = [];

  $rootScope.$on('event:loginRequired', function () {
    $rootScope.requests401 = [];
    $location.path('/login');
    loadingRequest.hide();
  });

  /**
   * On 'event:loginConfirmed', resend all the 401 requests.
   */
  $rootScope.$on('event:loginConfirmed', function () {
    var i,
      requests = $rootScope.requests401,
      retry = function (req) {
        $http(req.config).then(function (response) {
          req.deferred.resolve(response);
        });
      };

    for (i = 0; i < requests.length; i += 1) {
      retry(requests[i]);
    }
    $rootScope.requests401 = [];
  });

  /**
   * On 'logoutRequest' invoke logout on the server.
   */
  $rootScope.$on('event:logoutRequest', function () {
    httpHeaders.common['Authorization'] = null;
  });

  if ($location.url() == '' || $location.url() == '/') {
    AuthenticationService.ping(function (data) {
      UserService.setCurUser(data);
      $location.path(UserService.getHomePage());
    });
    return;
  }

  /**
   * On 'event:forbidden', resend all the 403 requests.
   */
  $rootScope.$on('event:forbidden', function (event, message) {
    var msg = !!(message) ? message : 'You don\' have right to take the action.';
    SweetAlert.swal($scope.dataKylin.alert.oops, 'Permission Denied: ' + msg, 'error');

  });

});
