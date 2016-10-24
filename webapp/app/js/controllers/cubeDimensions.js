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

KylinApp.controller('CubeDimensionsCtrl', function ($scope, $modal,MetaModel,cubesManager,SweetAlert ,kylinCommon) {

    $scope.cubeManager = cubesManager;
    // Available columns list derived from cube data model.
    $scope.availableColumns = {};

    // Columns selected and disabled status bound to UI, group by table.
    $scope.selectedColumns = {};

    // Available tables cache: 1st is the fact table, next are lookup tables.
    $scope.availableTables = [];


    /**
     * Helper func to get columns that dimensions based on, three cases:
     * 1. normal dimension: column array.
     * 2. derived dimension: derived columns array.
     * TODO new cube schema change
     */
    var dimCols = function (dim) {
        var referredCols = [];

        // Case 3.
        if (dim.derived && dim.derived.length) {
            referredCols = referredCols.concat(dim.derived);
        }

        // Case 2.
        //if (dim.hierarchy && dim.column.length) {
        //    referredCols = referredCols.concat(dim.column);
        //}

        // Case 1.
        if (!dim.derived && dim.column) {
            referredCols.push(dim.column);
        }

        return referredCols;
    };

    // Dump available columns plus column table name, whether is from lookup table.
    $scope.initColumns = function () {
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
            factSelectAvailable[cols[i].name] = {name:cols[i].name ,selected: false, disabled: false};
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
                lookupSelectAvailable[cols2[k].name] = {name:cols2[k].table+"_derived",selected: false, disabled: false};
            }

            $scope.availableColumns[lookups[j].table] = cols2;
            $scope.selectedColumns[lookups[j].table] = lookupSelectAvailable;
            if($scope.availableTables.indexOf(lookups[j].table)==-1){
                $scope.availableTables.push(lookups[j].table);
            }
        }
    };

    // Check column status: selected or disabled based on current cube dimensions.
    $scope.initColumnStatus = function () {
        angular.forEach($scope.cubeMetaFrame.dimensions, function (dim) {
            var cols = dimCols(dim);

            angular.forEach(cols, function (colName) {
                if(dim.derived){
                   $scope.selectedColumns[dim.table][colName] = {name:dim.name, selected: true, disabled: true,normal:"false"};
                }else{
                   $scope.selectedColumns[dim.table][colName] = {name:dim.name, selected: true, disabled: true,normal:"true"};
                }

            });
        });
    };

    // Initialize data for columns widget in auto-gen when add/edit cube.
    if ($scope.state.mode == 'edit') {
        $scope.initColumns();
    }

    // Initialize params for add/edit dimension.
    $scope.dimState = {
        editing: false,
        editingIndex: -1,
        filter: ''
    };

    // Init the dimension, dimension name default as the column key. TODO new cube schema change.
    var Dimension = function (name, table, selectedCols, dimType) {
        var origin = {name: '', table: table,derived:null,column:null};

        switch (dimType) {
            case 'normal':
                // Default name as 1st column name.
                if (table && selectedCols.length) {
                    origin.name = name;
                }

                origin.column = selectedCols[0];
                break;

            case 'derived':
                if (table && selectedCols.length) {
                    origin.name = name;
                }

                origin.derived = selectedCols;
                break;
        }

        return origin;
    };

    // Since old schema may be both derived and hierarchy. TODO new cube schema change.
    $scope.getDimType = function (dim) {
        var types = [];

        if (dim.derived && dim.derived.length) {
            types.push($scope.dataKylin.cube.cubeDSSwitchDer);
            //types.push('derived');
        }

        if (!types.length) {
          //types.push('normal');
            types.push($scope.dataKylin.cube.cubeDSSwitchNormal);
        }

        return types;
    };

    var dimList = $scope.cubeMetaFrame.dimensions;

    // Open add/edit dimension modal.
    $scope.openDimModal = function (dimType) {
        var modalInstance = $modal.open({
            templateUrl: 'addEditDimension.html',
            controller: cubeDimModalCtrl,
            backdrop: 'static',
            scope: $scope,
            resolve: {
                dimType: function () {
                    // For old schema compatibility, convert into array here. TODO new cube schema change.
                    return angular.isArray(dimType) ? dimType : [dimType];
                }
            }
        });

        modalInstance.result.then(function () {
            if (!$scope.dimState.editing) {
                $scope.doneAddDim();
            } else {
                $scope.doneEditDim();
            }

        }, function () {
            $scope.cancelDim();
        });
    };

    // Controller for cube dimension add/edit modal.
    var cubeDimModalCtrl = function ($scope, $modalInstance, dimType,SweetAlert,language) {
        $scope.dimType = dimType;
        $scope.dataKylin = language.getDataKylin();
        $scope.ok = function () {
            $modalInstance.close();
        };

        $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
        };

        $scope.checkDimension = function(){
            var errors = [];


            if($scope.dimType[0]=="derived"){
                if(!$scope.newDimension.derived.length){
                    errors.push($scope.dataKylin.alert.check_cube_dimensions_derived);
                }
                for(var i = 0;i<$scope.newDimension.derived.length;i++){
                    if($scope.newDimension.derived[i]===""){
                        errors.push($scope.dataKylin.alert.check_cube_dimensions_value);
                        break;
                    }
                }
                if($scope.newDimension.derived.length>1){
                    var _columns = angular.copy($scope.newDimension.derived).sort();
                    for(var i = 0;i<_columns.length-1;i++){
                        if(_columns[i]==_columns[i+1]&&_columns[i]!==""){
                            errors.push($scope.dataKylin.alert.check_cube_dimensions_duplicate+_columns[i]+".");
                        }
                    }
                }

            }

            var errorInfo = "";
            angular.forEach(errors,function(item){
                errorInfo+="\n"+item;
            });
            if(errors.length){
//                SweetAlert.swal('Warning!', errorInfo, '');
                SweetAlert.swal('', errorInfo, 'warning');
                return false;
            }else{
                return true;
            }
        }


    };

    $scope.addDim = function (dimType) {
        $scope.newDimension = Dimension('', [], dimType);

        $scope.openDimModal(dimType);
    };

    $scope.editDim = function (dim) {
        $scope.dimState.editingIndex = dimList.indexOf(dim);
        $scope.dimState.editing = true;

        // Make a copy of model will be editing.
        $scope.newDimension = angular.copy(dim);

        $scope.openDimModal($scope.getDimType(dim));
    };

    $scope.doneAddDim = function () {
        // Push new dimension which bound user input data.
        dimList.push(angular.copy($scope.newDimension));

        $scope.resetParams();
    };

    $scope.doneEditDim = function () {
        // Copy edited model to destination model.
        angular.copy($scope.newDimension, dimList[$scope.dimState.editingIndex]);

        $scope.resetParams();
    };

    $scope.cancelDim = function () {
        $scope.resetParams();
    };

    $scope.removeDim = function (dim) {
        dimList.splice(dimList.indexOf(dim), 1);

        var cols = dimCols(dim);
        angular.forEach(cols, function (colName) {
          if(dim.table==$scope.metaModel.model.fact_table){
             $scope.selectedColumns[dim.table][colName] = {name:colName,selected: false, disabled: false};
            }else{
             $scope.selectedColumns[dim.table][colName] = {name:dim.table+"_derived",selected: false, disabled: false};
            }
        });
    };

    $scope.resetParams = function () {
        $scope.dimState.editing = false;
        $scope.dimState.editingIndex = -1;

        $scope.newDimension = {};
    };

    // Open auto-gen dimension modal.
    $scope.openAutoGenModal = function (dimType) {
        // Init columns status.
        $scope.initColumnStatus();

        var modalInstance = $modal.open({
            templateUrl: 'autoGenDimension.html',
            controller: cubeAutoGenDimModalCtrl,
            backdrop: 'static',
            scope: $scope
        });

        modalInstance.result.then(function () {
            $scope.autoGenDims();
        }, function () {
            $scope.resetGenDims();
        });
    };

    // Controller for cube dimension auto-gen modal.
    var cubeAutoGenDimModalCtrl = function ($scope, $modalInstance,language,kylinCommon) {
      $scope.dataKylin = language.getDataKylin();
        $scope.ok = function () {
            $modalInstance.close();
        };

        $scope.cancel = function () {
            $modalInstance.dismiss('cancel');
        };
    };

    // Helper func to get the selected status in auto gen.
    $scope.getSelectedCols = function () {
        var selectedCols = {};
        angular.forEach($scope.selectedColumns, function (value, table) {
            angular.forEach(value, function (status, colName) {
                if (status.selected) {
                    if (!selectedCols[table]) {
                        selectedCols[table] = [];
                    }
                    var cols={
                        name:status.name,
                        col:colName,
                        normal:status.normal,
                        selected:status.selected
                    }
                    selectedCols[table].push(cols);
                }
            });

        });

        return selectedCols;
    };

    // Auto generate dimensions.
    $scope.autoGenDims = function () {
        var selectedCols = $scope.getSelectedCols();
        dimList=[];
        angular.forEach(selectedCols, function (cols, table) {
            if ($scope.metaModel.model.fact_table == table) {
                // Fact table: for each selected column, create one normal dimension.
                for (var i = 0; i < cols.length; i++) {
                    dimList.push(Dimension(cols[i].name, table, [cols[i].col], 'normal'));
                }
            } else {
                // Per lookup table, create one derived dimension for all its selected columns;

               for (var i = 0; i < cols.length; i++) {
                   if(cols[i].normal=="true"){
                        dimList.push(Dimension(cols[i].name, table, [cols[i].col], 'normal'));
                   }else{
                        dimList.push(Dimension(cols[i].name, table, [cols[i].col], 'derived'));
                   }
               }
            }
        });
         $scope.cubeMetaFrame.dimensions = dimList;
    };

    $scope.autoChange= function(table,name){
       if($scope.selectedColumns[table][name].selected==false){
           $scope.selectedColumns[table][name].normal=null;
           if(table==$scope.metaModel.model.fact_table){
               $scope.selectedColumns[table][name].name=name;
           }else{
               $scope.selectedColumns[table][name].name=table+"_derived";
           }
       }else{
          if($scope.metaModel.model.fact_table!=table){
               $scope.selectedColumns[table][name].normal="false";
           }
       }

    }
    $scope.checkAutoDimension=function(){
         var nameNull=false;
         angular.forEach($scope.selectedColumns, function (value, table) {
              angular.forEach(value, function (status, colName) {
                  if (status.selected) {
                    if(status.name==""){
                       SweetAlert.swal('', $scope.dataKylin.alert.warning_dimensions_name, 'warning');
                       nameNull=true;
                    }

                  }
              });

          });
          if(nameNull==true){
              return false;
          }else{
              return true;
          }
    }

    $scope.addNewDimension = function(newDimension){
       if(newDimension.derived==null){
         newDimension.derived=[];
       }
       newDimension.derived.push('');
    }

    // Just reset the selected status of columns.
    $scope.resetGenDims = function () {
        angular.forEach($scope.selectedColumns, function (value, table) {
            angular.forEach(value, function (status, colName) {
                       status.selected=false;
                       status.normal=null;
                    });
                });
    };

    // Check whether there is column conflicts.
    $scope.dimConflicts = [];

    $scope.$watch('cubeMetaFrame.dimensions', function (newVal, oldVal) {
        if (!newVal || !newVal.length) {
            return;
        }

        var referredCols = {};

        angular.forEach(newVal, function (curDim) {
            var table = curDim.table;
            var cols = dimCols(curDim);

            for (var i = 0; i < cols.length; i++) {
                var key = table + '.' + cols[i];

                if (!referredCols[key]) {
                    referredCols[key] = [];
                }

                referredCols[key].push({id: curDim.id, name: curDim.name});
            }
        });

        var conflicts = [];

        angular.forEach(referredCols, function (dims, key) {
            if (dims.length > 1) {
                // More than 1 dimensions has referred this column.
                var colInfo = key.split('.');
                conflicts.push({table: colInfo[0], column: colInfo[1], dims: dims});
            }
        });

        $scope.dimConflicts = conflicts;
    }, true);

    if ($scope.state.mode == 'edit') {
        $scope.$on('$destroy', function () {
           // $scope.dimensionsAdapter();
            // Emit dimensions edit event in order to re-generate row key.
            $scope.$emit('DimensionsEdited');
        });
    }
});
