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

KylinApp
  .controller('ProjectMetaCtrl', function ($scope, $q, ProjectService, QueryService, modelsManager, $log, CubeService, CubeDescService,SqlModel) {

    $scope.modelsManager = modelsManager;
    $scope.selectedSrcDb = [];
    $scope.selectedSrcTable = {};
    $scope.readyCubes = [];
    $scope.readyCubeDescs = [];
    $scope.selectedModel ='';
    $scope.availableModels = [];


    $scope.queryObject = {
      selectedModel:'',
      avaModels:[]
    };

    $scope.sqlModel = SqlModel;

    $scope.columnIcon="badge";
    $scope.dimensionIcon = "badge label-info cube-dimension";
    $scope.measureIcon = "badge label-info cube-measure";
    $scope.dimensionMeasureIcon = "badge label-info cube-dimension-measure";
    $scope.pkIcon = "badge label-primary cube-pk";
    $scope.fkIcon = "badge label-primary cube-fk";

    $scope.factIcon = "badge label-info cube-fact";
    $scope.lookupIcon = "badge label-info cube-lookup";


    $scope.showSelected = function (table) {
      if (table.uuid) {
        $scope.selectedSrcTable = table;
      }
      else {
        $scope.selectedSrcTable.selectedSrcColumn = table;
      }
    }

    $scope.doubleClick = function (branch) {
      if (!branch.parent_uid) {
        return;
      }

      var selectTable = false;
      if (branch.data && branch.data.table_TYPE == "TABLE") {
        selectTable = true;
      }

      if (angular.isUndefined($scope.$parent.queryString)) {
        $scope.$parent.queryString = '';
      }
      if (selectTable)
        $scope.$parent.queryString += (branch.data.table_NAME + ' ');
      else
        $scope.$parent.queryString += (branch.data.table_NAME + '.' + branch.data.column_NAME + ' ');


    }
    //$scope.col_defs = [
    //  {
    //    field: "Desc",
    //    cellTemplate: "<span class='badge' ng-class=\"{'ng-hide': t=='PK'||t=='FK'||t=='D'||t=='M','label-success':t=='FACT'||t=='LOOKUP'}\" style='margin-right:3px;' ng-repeat='t in row.branch[col.field]'>{{t}}</span>"
    //  }
    //];


    $scope.modelChanged = function(){
      $scope.sqlModel.tableMeasures = {};
      $scope.sqlModel.dimensions = [];
      $scope.sqlModel.measures = [];
      $scope.sqlModel.selectedDimensions = [];
      $scope.sqlModel.selectedMeasures = [];
      $scope.sqlModel.columnDimensions = {};

      if($scope.selectedModel===""||!$scope.selectedModel){
        return;
      }

      $scope.sqlModel.model = $scope.modelsManager.getModel($scope.selectedModel);

      for(var i=0;i<$scope.readyCubeDescs.length;i++){
        var cubeDesc = $scope.readyCubeDescs[i];
        var model = $scope.modelsManager.getModel(cubeDesc.model_name);
        if($scope.selectedModel !== model.name){
          continue;
        }
        var factTableName = model.fact_table;

        for(var k=0;k<cubeDesc.dimensions.length;k++){
          var dimension = cubeDesc.dimensions[k];


          if(dimension.column && dimension.derived == null){

            //do not add duplicate
            if(!$scope.sqlModel.columnDimensions[dimension.column]){
              $scope.sqlModel.dimensions.push({
                name:dimension.column,
                table:dimension.table,
                isDimension:true
              });
            }
            $scope.sqlModel.columnDimensions[dimension.column]={
              name:dimension.column,
              table:dimension.table,
              isDimension:true
            }
          }
          if(dimension.derived&&dimension.derived.length>=1){
            for(var m=0;m<dimension.derived.length;m++){

              if(!$scope.sqlModel.columnDimensions[dimension.derived[m]]) {
                $scope.sqlModel.dimensions.push({
                  name: dimension.derived[m],
                  table: dimension.table,
                  isDimension: true
                });
              }

              $scope.sqlModel.columnDimensions[dimension.derived[m]]={
                name:dimension.derived[m],
                table:dimension.table
              }


            }
          }
        }

        //$scope.tableMeasures
        if(!$scope.sqlModel.tableMeasures[factTableName]){
          $scope.sqlModel.tableMeasures[factTableName] = {};
        }

        for(var j=0;j<cubeDesc.measures.length;j++){
          var measure = cubeDesc.measures[j];
          var expression = measure.function.expression;
          var column = measure.function.parameter.value;
          var type = measure.function.parameter.type;

          var tableMeasureItem = {
            mea_expression:expression,
            mea_type:type,
            mea_value:column,
            mea_display:expression+'('+column+')',
            mea_next_parameter:measure.function.parameter.next_parameter,
            isMeasure:true
          }

          if(!$scope.sqlModel.tableMeasures[factTableName][column] || !$scope.sqlModel.tableMeasures[factTableName][column][expression]){
            $scope.sqlModel.measures.push(tableMeasureItem);
          }
          //duplicate check with map
          if($scope.sqlModel.tableMeasures[factTableName]){
            $scope.sqlModel.tableMeasures[factTableName][column] = {};
            $scope.sqlModel.tableMeasures[factTableName][column][expression]=tableMeasureItem;
          }

        }
      }

    }

    $scope.dimensionAccept = {
      accept: function(dragEl) {
        console.log("dimension in");
        if(dragEl.hasClass("drag-dimension")){
          console.log("is dimension");
          return true;
        }
        else{
          console.log("not dimension");
          return false;
        }
      }

    }

    $scope.measureAccept = {
    accept: function(dragEl) {
      console.log("measure in");
        if(dragEl.hasClass("drag-measure")){
          console.log("is measure");
          return true;
        }else{
          console.log("not measure");
          return false;
        }
      }
    }


    $scope.genSql = function(){

      //valid rule like topn validate
      $scope.sqlModel.init();
      $scope.sqlModel.getSql();

    }


    $scope.projectMetaLoad = function () {
      $scope.readyCubes = [];
      $scope.readyCubeDescs = [];
      $scope.availableModels = [];
      $scope.selectedSrcDb = [];
      $scope.modelChanged();

      $scope.selectedModel = "";

      var defer = $q.defer();
      if (!$scope.projectModel.getSelectedProject()) {
        return;
      }

      var queryParam = {
        projectName: $scope.projectModel.selectedProject
      }

      $scope.loading = true;
      var cubesPromise = [];
      var cubeDescPromise = [];
      $scope.modelsManager.list(queryParam).then(function (resp) {
        defer.resolve(resp);
        $scope.availableModels = $scope.modelsManager.models;
        modelsManager.loading = false;
        return defer.promise;
      }).then(function () {

        cubesPromise.push(CubeService.list(queryParam, function (_cubes) {
          for (var i = 0; i < _cubes.length; i++) {
            if (_cubes[i].status == 'READY') {
              $scope.readyCubes.push(_cubes[i]);
            }
          }
        }).$promise)

        $q.all(cubesPromise).then(function () {
          for (var i = 0; i < $scope.readyCubes.length; i++) {
            cubeDescPromise.push(CubeDescService.query({cube_name: $scope.readyCubes[i].name}, {}, function (detail) {
              $scope.readyCubeDescs.push(detail[0]);
            }).$promise)
          }
          $q.all(cubeDescPromise).then(function(){
              $scope.loading = false;
              defer.resolve();
          })

        })

      })
      return defer.promise;
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      $scope.projectMetaLoad();
    });


    $scope.columnTypeFormat = function (typeName) {
      if (typeName) {
        return "(" + $scope.trimType(typeName) + ")";
      } else {
        return "";
      }
    }
    $scope.trimType = function (typeName) {
      if (typeName.match(/VARCHAR/i)) {
        typeName = "VARCHAR";
      }

      return typeName.trim().toLowerCase();
    }

  });

