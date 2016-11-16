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

KylinApp.controller('ModelDimensionsCtrl', function ($scope, $modal, MetaModel, modelsManager, modelsEdit, CubeDescService, TableService) {
    $scope.modelsManager = modelsManager;
    $scope.selectedColumns = {};

    // Available tables cache: 1st is the fact table, next are lookup tables.
    $scope.availableTables = [];

    // Dump available columns plus column table name, whether is from lookup table.
    $scope.initColumns = function () {
        $scope.availableTables.push(modelsManager.selectedModel.fact_table);
//        var table=modelsManager.getTable();*/
        var lookups = modelsManager.selectedModel.lookups;
        for (var j = 0; j < lookups.length; j++) {
            $scope.availableTables.push(lookups[j].table);
        }
        angular.forEach(modelsManager.selectedModel.dimensions,function(dim){
            if($scope.availableTables.indexOf(dim.table)==-1){
                modelsManager.selectedModel.dimensions = modelsManager.selectedModel.dimensions==null?[]:modelsManager.selectedModel.dimensions;
                modelsManager.selectedModel.dimensions.splice(modelsManager.selectedModel.dimensions.indexOf(dim),1);
            }
        });
        for(var i = 0;i<$scope.availableTables.length;i++){
            var tableInUse = _.some(modelsManager.selectedModel.dimensions,function(item){
                return item.table == $scope.availableTables[i];
            });
            if(!tableInUse){
                modelsManager.selectedModel.dimensions = modelsManager.selectedModel.dimensions==null?[]:modelsManager.selectedModel.dimensions;
                modelsManager.selectedModel.dimensions.push(new Dimension($scope.availableTables[i]));
            }
        }
        for (var j = 0; j < $scope.availableTables.length; j++) {
            var cols2 = $scope.getColumnsByTable($scope.availableTables[j]);
            // Initialize selected available.
            var SelectAvailable = {};
            for (var k = 0; k < cols2.length; k++) {
            // Default not selected and not disabled.
                SelectAvailable[cols2[k].name] = {name:cols2[k].name,selected: false,disabled:false};
            }
            SelectAvailable.all=false;
            SelectAvailable.disabled=false;
            SelectAvailable.open=false;
            SelectAvailable.sortFlag = ''
            SelectAvailable.sortIcon = 'fa fa-unsorted';
            $scope.selectedColumns[$scope.availableTables[j]] = SelectAvailable;
        }
        angular.forEach(modelsManager.selectedModel.dimensions, function (dim) {
            angular.forEach(dim.columns, function (column) {
                $scope.selectedColumns[dim.table][column].selected=true;
            });
        });
        angular.forEach($scope.usedDimensionsCubeMap, function (dim,dimension) {
            angular.forEach(dim, function (col,column) {
                 $scope.selectedColumns[dimension][column].disabled=true;
            });
        });
        angular.forEach($scope.selectedColumns,function(value,table){
            var all=true;
            var disabled=true;
            var open=false;
            angular.forEach(value,function(col){
                if(typeof col=="object"){
                    if(col.selected==false){
                        all=false;
                    }else{
                         open=true;
                    }
                    if(col.disabled==false){
                         disabled=false;
                    }
                }
            });
             $scope.selectedColumns[table].disabled=disabled;
            $scope.selectedColumns[table].all=all;
            $scope.selectedColumns[table].open=open;
        });
        var column=$scope.getColumnsByTable(modelsManager.selectedModel.fact_table);
    };


    var Dimension = function(table){
        this.table = table;
        this.columns = [];
    }


   // Initialize data for columns widget in auto-gen when add/edit cube.
    if ($scope.state.mode == 'edit') {
        $scope.initColumns();
    };


    $scope.Change= function(table,name,index){
       if($scope.selectedColumns[table][name].selected==false){
          $scope.selectedColumns[table].all=false;
          modelsManager.selectedModel.dimensions[index].columns.splice(modelsManager.selectedModel.dimensions[index].columns.indexOf(name),1);
       }else{
          var all=true;
          angular.forEach($scope.selectedColumns[table],function(col){
             if(col.selected==false&&typeof col=="object"){
                 all=false;
             }
          });
          $scope.selectedColumns[table].all=all;
          modelsManager.selectedModel.dimensions[index].columns.push(name);
       }
    }

    $scope.ChangeAll= function(table,index){
        angular.forEach($scope.selectedColumns[table],function(col){
            if($scope.cubesLength>0){
                if(typeof col==="object"&&col.disabled==false){
                    var local=modelsManager.selectedModel.dimensions[index].columns.indexOf(col.name);
                    if($scope.selectedColumns[table].all==true&&col.selected==false){
                        col.selected=true;
                        modelsManager.selectedModel.dimensions[index].columns.push(col.name);
                    }
                    if($scope.selectedColumns[table].all==false&&col.selected==true){
                        col.selected=false;
                        modelsManager.selectedModel.dimensions[index].columns.splice(local,1);
                    }
                }
            }else{
                if(typeof col==="object"){
                    if($scope.selectedColumns[table].all==true){
                        if(col.selected == false){
                           col.selected=true;
                           modelsManager.selectedModel.dimensions[index].columns.push(col.name);
                        }
                    }else{
                        col.selected=false;
                        modelsManager.selectedModel.dimensions[index].columns.splice(0,1);
                    }
                }
            }
        });
    }


});
