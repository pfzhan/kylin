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
  .controller('ProjectMetaCtrl', function ($scope, $q, ProjectService, QueryService, modelsManager, $log, CubeService, CubeDescService) {

    $scope.modelsManager = modelsManager;
    $scope.selectedSrcDb = [];
    $scope.selectedSrcTable = {};
    $scope.readyCubes = [];
    $scope.readyCubeDescs = [];
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


    $scope.projectMetaLoad = function () {
      var defer = $q.defer();
      $scope.selectedSrcDb = [];
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

            for(var i=0;i<$scope.readyCubeDescs.length;i++){
              var cubeDesc = $scope.readyCubeDescs[i];
              var model = $scope.modelsManager.getModel(cubeDesc.model_name);
              var factTable = model.fact_table;

              //recursive parameter not included
              for(var j=0;j<cubeDesc.measures.length;j++){
                var measure = cubeDesc.measures[j];
                var expression = measure.function.expression;
                var column = measure.function.parameter.value;
                if(!$scope.tableColumnMeasuresMap[factTable]){
                  $scope.tableColumnMeasuresMap[factTable] ={};
                  $scope.tableColumnMeasuresMap[factTable][column] = [];
                  $scope.tableColumnMeasuresMap[factTable][column].push(expression);
                }else{
                  if($scope.tableColumnMeasuresMap[factTable][column]){
                    $scope.tableColumnMeasuresMap[factTable][column].push(expression);
                  }else{
                    $scope.tableColumnMeasuresMap[factTable][column] = [];
                    $scope.tableColumnMeasuresMap[factTable][column].push(expression);
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
                      }
                      if(tableTags.indexOf("LOOKUP")!=-1){
                        tableIcon = $scope.lookupIcon;
                      }

                      var _table_node = {
                        Name: _table.name,
                        data: _table,
                        icon: tableIcon,
                        onSelect: function (branch) {
                          // set selected model
                          $scope.selectedSrcTable = branch.data;
                        }
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

                        _column_node_list.push({
                          Name: _column.name + $scope.columnTypeFormat(_column.type_NAME),
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

