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

KylinApp.controller('ModelSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, $q, AuthenticationService, $filter, ModelService, MetaModel, CubeDescModel, CubeList, TableModel, ProjectModel, $log, SweetAlert, modelsManager,language) {

  $scope.modelsManager = modelsManager;
  $scope.projectModel = ProjectModel;
  $scope.projects = [];
  $scope.newDimension = null;
  $scope.newMeasure = null;

  $scope.forms = {};

  // ~ init
  if (!$scope.state) {
    $scope.state = {mode: "view"};
  }

  if(!$scope.partitionColumn){
    $scope.partitionColumn ={
      "hasSeparateTimeColumn" : false
    }
  }

  $scope.dataKylin =  language.getDataKylin();
  $scope.wizardSteps =  [
    {title: $scope.dataKylin.model.schema[0], src: 'partials/modelDesigner/model_info.html', isComplete: false, form: 'model_info_form'},
    {title: $scope.dataKylin.model.schema[1], src: 'partials/modelDesigner/data_model.html', isComplete: false, form: 'data_model_form'},
    {
      title: $scope.dataKylin.model.schema[2],
      src: 'partials/modelDesigner/model_dimensions.html',
      isComplete: false,
      form: 'model_dimensions_form'
    },
    {
      title: $scope.dataKylin.model.schema[3],
      src: 'partials/modelDesigner/model_measures.html',
      isComplete: false,
      form: 'model_measure_form'
    },
    {
      title: $scope.dataKylin.model.schema[4],
      src: 'partials/modelDesigner/conditions_settings.html',
      isComplete: false,
      form: 'model_setting_form'
    }
  ];

  $scope.curStep = $scope.wizardSteps[0];

  $scope.$on("finish",function (event,data) {
    $scope.dataKylin =  language.getDataKylin();
    for(var i in $scope.wizardSteps){
      $scope.wizardSteps[i].title = $scope.dataKylin.model.schema[i];
    }
  });

  //init modelsManager
  if ($scope.state.mode == "edit") {
    var defer = $q.defer();
    var queryParam = {};
    if (!$scope.projectModel.isSelectedProjectValid()) {
      return;
    }
    queryParam.projectName = $scope.projectModel.selectedProject;
    modelsManager.list(queryParam).then(function (resp) {
      defer.resolve(resp);
      modelsManager.loading = false;
      return defer.promise;
    });
  }

  $scope.$watch('model', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if ($scope.modelMode == "editExistModel" && newValue && !newValue.project) {
      initProject();
    }

  });

  // ~ public methods
  $scope.filterProj = function (project) {
    return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project, $scope.permissions.ADMINISTRATION.mask);
  };

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.open = function ($event) {
    $event.preventDefault();
    $event.stopPropagation();

    $scope.opened = true;
  };
  $scope.preView = function () {
    var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
    if (stepIndex >= 1) {
      $scope.curStep.isComplete = false;
      $scope.curStep = $scope.wizardSteps[stepIndex - 1];
    }
  };

  $scope.nextView = function () {
    var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
    if (stepIndex < ($scope.wizardSteps.length - 1)) {
      $scope.curStep.isComplete = true;
      $scope.curStep = $scope.wizardSteps[stepIndex + 1];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  };

  $scope.checkForm = function (stepIndex) {
    // do not check for Prev Step
    if (stepIndex + 1 < $scope.curStep.step) {
      return true;
    }

    if (!$scope.curStep.form) {
      return true;
    }
    if ($scope.state.mode === 'view') {
      return true;
    }
    else {
      //form validation
      if ($scope.forms[$scope.curStep.form].$invalid) {
        $scope.forms[$scope.curStep.form].$submitted = true;
        return false;
      } else {
        //business rule check
        switch ($scope.curStep.form) {
          case 'data_model_form':
            return $scope.check_data_model();
            break;
          case 'model_info_form':
            return $scope.check_model_info();
            break;
          case 'model_dimensions_form':
            return $scope.check_model_dimensions();
            break;
          case 'model_measure_form':
            return $scope.check_model_measure();
            break;
          case 'model_setting_form':
            return $scope.check_model_setting();
            break;
          default:
            return true;
            break;
        }
      }
    }
  };

  $scope.check_model_info = function () {

    var modelName = $scope.modelsManager.selectedModel.name.toUpperCase();
    var models = $scope.modelsManager.modelNameList;
    if ($scope.modelMode=="addNewModel") {
      if(models.indexOf(modelName) != -1 || models.indexOf(modelName.toLowerCase()) !=-1){
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.warning_model_part_one + modelName + $scope.dataKylin.alert.warning_model_part_two, 'warning');
        return false;
      }
    }
    return true;
  }

  /*
   * lookups can't be null
   */
  $scope.check_data_model = function () {
    var errors = [];
//        if(!modelsManager.selectedModel.lookups.length){
//            errors.push("No lookup table defined");
//        }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  }

  /*
   * dimensions validation
   * 1.dimension can't be null
   */
  $scope.check_model_dimensions = function () {

    var errors = [];
    if (!modelsManager.selectedModel.dimensions.length) {
      errors.push("No dimensions defined.");
    }
    var isDimensionDefined = false;
    angular.forEach(modelsManager.selectedModel.dimensions, function (_dimension) {
      if(_dimension.columns && _dimension.columns.length){
        isDimensionDefined = true;
      }
    });
    if(!isDimensionDefined){
      errors.push($scope.dataKylin.alert.check_model_dimension);
    }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal($scope.dataKylin.alert.oops, errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  };

  /*
   * dimensions validation
   * 1.metric can't be null
   */
  $scope.check_model_measure = function () {

    var errors = [];
    if (!modelsManager.selectedModel.metrics || !modelsManager.selectedModel.metrics.length) {
      errors.push($scope.dataKylin.alert.check_model_measure_define);
    }
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }

  };
  $scope.check_model_setting = function () {
    var errors = [];
    var errorInfo = "";
    angular.forEach(errors, function (item) {
      errorInfo += "\n" + item;
    });
    if (errors.length) {
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    } else {
      return true;
    }
  }


  $scope.goToStep = function (stepIndex) {
    if ($scope.modelMode == "addNewModel") {
      /*if (stepIndex + 1 >= $scope.curStep.step) {
        return;
      }*/
    }
    for (var i = 0; i < $scope.wizardSteps.length; i++) {
      if (i <= stepIndex) {
        $scope.wizardSteps[i].isComplete = true;
      } else {
        $scope.wizardSteps[i].isComplete = false;
      }
    }
    if (stepIndex < ($scope.wizardSteps.length)) {
      $scope.curStep = $scope.wizardSteps[stepIndex];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  }

  // ~ private methods
  function initProject() {

  }
});
