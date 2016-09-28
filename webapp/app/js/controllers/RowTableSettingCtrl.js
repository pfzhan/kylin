/**
 * Created by luguosheng on 16/9/26.
 */
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

KylinApp.controller('RowTableSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,RowTablesService,cubesManager,modelsManager,SweetAlert) {
  $scope.availableColumns = {};
  $scope.selectedColumns = {};

  // Available tables cache: 1st is the fact table, next are lookup tables.
 $scope.availableTables = [];
 if($scope.state.mode=="edit") {

   var factTable = $scope.metaModel.model.fact_table;

   // At first dump the columns of fact table.
//        var cols = $scope.getColumnsByTable(factTable);
   var cols = $scope.getDimColumnsByTable(factTable);

   // Initialize selected available.
   var factSelectAvailable = {};

   for (var i = 0; i < cols.length; i++) {
     cols[i].table = factTable;
     cols[i].isLookup = false;

     // Default not selected and not disabled.
     factSelectAvailable[cols[i].name] = {selected: false, disabled: false};
   }

   $scope.availableColumns[factTable] = cols;
   $scope.selectedColumns[factTable] = factSelectAvailable;
   $scope.availableTables.push(factTable);

   // Then dump each lookup tables.
   var lookups = $scope.metaModel.model.lookups;

   for (var j = 0; j < lookups.length; j++) {
     var cols2 = $scope.getDimColumnsByTable(lookups[j].table);

     // Initialize selected available.
     var lookupSelectAvailable = {};

     for (var k = 0; k < cols2.length; k++) {
       cols2[k].table = lookups[j].table;
       cols2[k].isLookup = true;


       // Default not selected and not disabled.
       lookupSelectAvailable[cols2[k].name] = {selected: false, disabled: false};
     }

     $scope.availableColumns[lookups[j].table] = cols2;
     $scope.selectedColumns[lookups[j].table] = lookupSelectAvailable;
     if ($scope.availableTables.indexOf(lookups[j].table) == -1) {
       $scope.availableTables.push(lookups[j].table);
     }
   }
   var rowTableColumns = [];
   for (var i in $scope.availableColumns) {
     rowTableColumns = rowTableColumns.concat($scope.availableColumns[i]);
   }


   var saveData = {};
   saveData.columns = [];
   for (var i = 0; i < rowTableColumns.length; i++) {
     var columnObj = {};
     columnObj.index = "discrete";
     columnObj.encoding = "var";
     columnObj.table = rowTableColumns[i].table;
     columnObj.column = rowTableColumns[i].name;
     saveData.columns.push(columnObj);
   }

   $scope.rowTableColumns = saveData;
 }
    $scope.refreshRowTablesIndex = function (item, val) {
      $scope.$emit('RowTableEdited', $scope.rowTableColumns);
    }
    if($scope.isEdit||$scope.state.mode=="view"){
      $scope.loadRowTable = function () {
        RowTablesService.getRowTableInfo({rowTableName: $scope.state.cubeName}, {}, function (request) {
          if(request){
            $scope.rowTableColumns = request;
          }

        })
      }
      $scope.loadRowTable();
    }


});
