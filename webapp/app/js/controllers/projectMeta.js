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
    //table dimensions
    $scope.tableDimensions = {};
    //table measures
    $scope.tableMeasures = {};
    //table&column map measure
    $scope.tableColumnMeasuresMap = {};

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
      $scope.tableColumnMeasuresMap = {};
      $scope.sqlModel.dimensions = [];
      $scope.sqlModel.measures = [];
      $scope.sqlModel.selectedDimensions = [];
      $scope.sqlModel.selectedMeasures = [];

      if($scope.selectedModel===""||!$scope.selectedModel){
        return;
      }


      for(var i=0;i<$scope.readyCubeDescs.length;i++){
        var cubeDesc = $scope.readyCubeDescs[i];
        var model = $scope.modelsManager.getModel(cubeDesc.model_name);
        if($scope.selectedModel !== model.name){
          continue;
        }
        var factTableName = model.fact_table;

        //gen sql
        if(!$scope.sqlModel.tableModelJoin[factTableName]){
          $scope.sqlModel.tableModelJoin[factTableName] ={};
        }
        for(var i=0;i<model.lookups.length;i++){
          var lookup = model.lookups[i];
          $scope.sqlModel.tableModelJoin[factTableName][lookup.table] = {};
          $scope.sqlModel.tableModelJoin[factTableName][lookup.table]=lookup.join;
        }


        for(var k=0;k<cubeDesc.dimensions.length;k++){
          var dimension = cubeDesc.dimensions[k];
          if(!$scope.sqlModel.tableDimensions[dimension.table]){
            $scope.sqlModel.tableDimensions[dimension.table] = [];
          }

          if(dimension.column && dimension.derived == null){
            $scope.sqlModel.tableDimensions[dimension.table].push(dimension.column);
            $scope.sqlModel.dimensions.push({
              name:dimension.column,
              table:dimension.table
            });
          }
          if(dimension.derived&&dimension.derived.length>=1){
            for(var m=0;m<dimension.derived.length;m++){
              $scope.sqlModel.tableDimensions[dimension.table].push(dimension.derived[m]);
              $scope.sqlModel.dimensions.push({
                name:dimension.derived[m],
                table:dimension.table
              });
            }
          }
        }

        //$scope.tableMeasures
        if(!$scope.sqlModel.tableMeasures[factTableName]){
          $scope.sqlModel.tableMeasures[factTableName] = [];
        }

        //recursive parameter not included
        for(var j=0;j<cubeDesc.measures.length;j++){
          var measure = cubeDesc.measures[j];
          var expression = measure.function.expression;
          var column = measure.function.parameter.value;
          var type = measure.function.parameter.type;

          var tableMeasureItem = {
            mea_expression:expression,
            mea_type:type,
            mea_value:column,
            mea_display:expression+'('+column+')'
          }

          $scope.sqlModel.measures.push(tableMeasureItem);

          //TO-DO duplicate check, topn
          if($scope.sqlModel.tableMeasures[factTableName]){
            $scope.sqlModel.tableMeasures[factTableName].push(tableMeasureItem);
          }


          if(!$scope.tableColumnMeasuresMap[factTableName]){
            $scope.tableColumnMeasuresMap[factTableName] ={};
            $scope.tableColumnMeasuresMap[factTableName][column] = [];
            $scope.tableColumnMeasuresMap[factTableName][column].push(expression);
          }else{
            if($scope.tableColumnMeasuresMap[factTableName][column]){
              $scope.tableColumnMeasuresMap[factTableName][column].push(expression);
            }else{
              $scope.tableColumnMeasuresMap[factTableName][column] = [];
              $scope.tableColumnMeasuresMap[factTableName][column].push(expression);
            }
          }
        }
      }

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

            for(var i=0;i<$scope.readyCubeDescs.length;i++){
              var cubeDesc = $scope.readyCubeDescs[i];
              var model = $scope.modelsManager.getModel(cubeDesc.model_name);
              if($scope.selectedModel !== model){
                continue;
              }
              var factTableName = model.fact_table;

              //gen sql
              if(!$scope.sqlModel.tableModelJoin[factTableName]){
                $scope.sqlModel.tableModelJoin[factTableName] ={};
              }
              for(var i=0;i<model.lookups.length;i++){
                var lookup = model.lookups[i];
                $scope.sqlModel.tableModelJoin[factTableName][lookup.table] = {};
                $scope.sqlModel.tableModelJoin[factTableName][lookup.table]=lookup.join;
              }


              //$scope.tableMeasures
              if(!$scope.sqlModel.tableMeasures[factTableName]){
                $scope.sqlModel.tableMeasures[factTableName] = [];
              }


              //recursive parameter not included
              for(var j=0;j<cubeDesc.measures.length;j++){
                var measure = cubeDesc.measures[j];
                var expression = measure.function.expression;
                var column = measure.function.parameter.value;
                var type = measure.function.parameter.type;

                var tableMeasureItem = {
                  mea_expression:expression,
                  mea_type:type,
                  mea_value:column
                }

                //TO-DO duplicate check, topn
                if($scope.sqlModel.tableMeasures[factTableName]){
                  $scope.sqlModel.tableMeasures[factTableName].push(tableMeasureItem);
                }


                if(!$scope.tableColumnMeasuresMap[factTableName]){
                  $scope.tableColumnMeasuresMap[factTableName] ={};
                  $scope.tableColumnMeasuresMap[factTableName][column] = [];
                  $scope.tableColumnMeasuresMap[factTableName][column].push(expression);
                }else{
                  if($scope.tableColumnMeasuresMap[factTableName][column]){
                    $scope.tableColumnMeasuresMap[factTableName][column].push(expression);
                  }else{
                    $scope.tableColumnMeasuresMap[factTableName][column] = [];
                    $scope.tableColumnMeasuresMap[factTableName][column].push(expression);
                  }
                }
              }
            }

            QueryService.getTables({project: $scope.projectModel.getSelectedProject()}, {}, function (tables) {
              var tableMap = [];
              angular.forEach(tables, function (table) {
                if (!tableMap[table.table_SCHEM]) {
                  tableMap[table.table_SCHEM] = [];
                }
                table.name = table.table_NAME;
                angular.forEach(table.columns, function (column, index) {
                  column.name = column.column_NAME;
                });
                tableMap[table.table_SCHEM].push(table);
              });

              for (var key in  tableMap) {

                var tables = tableMap[key];
                var _db_node = {
                  Name: key,
                  drag: false,
                  data: tables,
                  onSelect: function (branch) {
                    $log.info("db " + key + "selected");
                  }
                }

                var _table_node_list = [];
                angular.forEach(tables, function (_table) {
                    var tableIcon = "fa fa-table";

                    var tableName = _table.table_SCHEM + "." + _table.name;
                    //get fact lookup info
                    var tableTags = $scope.modelsManager.getTableDesc(tableName);
                    if(tableTags.indexOf("FACT")!=-1){
                      tableIcon = $scope.factIcon;
                      _table.isFactTable = true;
                    }
                    if(tableTags.indexOf("LOOKUP")!=-1){
                      tableIcon = $scope.lookupIcon;
                    }

                    var _table_node = {
                      Name: _table.name,
                      drag: true,
                      data: _table,
                      icon: tableIcon,
                      onSelect: function (branch) {
                        // set selected model
                        $scope.selectedSrcTable = branch.data;
                      }
                    }

                    if(!$scope.sqlModel.tableDimensions[tableName]){
                      $scope.sqlModel.tableDimensions[tableName] = [];
                    }


                    var _column_node_list = [];
                    angular.forEach(_table.columns, function (_column) {
                      var columnIcon = "";

                      var columnTags = $scope.modelsManager.getColumnDesc(tableName, _column.name);
                      if(columnTags.indexOf("D")!=-1){
                        columnIcon = $scope.dimensionIcon;
                      }
                      if(columnTags.indexOf("M")!=-1){
                        columnIcon = $scope.measureIcon;
                      }
                      if(columnTags.indexOf("M")!=-1 && columnTags.indexOf("D")!=-1){
                        columnIcon = $scope.dimensionMeasureIcon;
                      }
                      if(columnTags.indexOf("PK")!=-1){
                        columnIcon = $scope.pkIcon;
                      }

                      if(columnTags.indexOf("FK")!=-1){
                        columnIcon = $scope.fkIcon;
                      }

                      if($scope.tableColumnMeasuresMap[tableName]){
                        if($scope.tableColumnMeasuresMap[tableName][_column.name]){
                          columnTags = columnTags.concat($scope.tableColumnMeasuresMap[tableName][_column.name]);
                        }

                      }
                      if(columnTags.indexOf("D")!=-1){
                        $scope.sqlModel.tableDimensions[tableName].push(name);

                      }

                      _column_node_list.push({
                        Name: _column.name + $scope.columnTypeFormat(_column.type_NAME),
                        drag:false,
                        data: _column,
                        icon: columnIcon,
                        onSelect: function (branch) {
                          // set selected model
                          $log.info("selected column info:" + _column.name);
                        }
                      });
                    });
                    _table_node.children = _column_node_list;
                    _table_node_list.push(_table_node);

                    _db_node.children = _table_node_list;
                  }
                );

                $scope.selectedSrcDb.push(_db_node);
              }

              $scope.loading = false;
              defer.resolve();
            })
          })

        })

      })
      return defer.promise;
    };


    $scope.optionsList1 = {
      accept: function(dragEl) {
        return true;
      }
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

