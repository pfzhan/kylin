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


KylinApp.controller('ModelEditCtrl', function ($scope, $q, $routeParams, $location, $templateCache, $interpolate, MessageService, TableService, CubeDescService, ModelService, loadingRequest, SweetAlert,$log,cubeConfig,CubeDescModel,ModelDescService,MetaModel,TableModel,ProjectService,ProjectModel,modelsManager,kylinCommon,VdmUtil,CubeService,modelsEdit) {
    //add or edit ?
    var absUrl = $location.absUrl();
    $scope.modelMode = absUrl.indexOf("/models/add")!=-1?'addNewModel':absUrl.indexOf("/models/edit")!=-1?'editExistModel':'default';

    if($scope.modelMode=="addNewModel"&&ProjectModel.selectedProject==null){
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_project, 'warning');
        $location.path("/models");
    }

    $scope.modelsManager = modelsManager;
    $scope.usedDimensionsCubeMap = {};
    $scope.usedMeasuresCubeMap = {};
    $scope.cubeConfig = cubeConfig;

    $scope.getPartitonColumns = function(tableName){
        var columns = _.filter($scope.getColumnsByTable(tableName),function(column){
            return column.datatype==="date"||column.datatype==="timestamp"||column.datatype==="string"||column.datatype.startsWith("varchar")||column.datatype==="bigint"||column.datatype==="int"||column.datatype==="integer";
        });
        return columns;
    };

    $scope.getPartitonTimeColumns = function(tableName){
      var columns = _.filter($scope.getColumnsByTable(tableName),function(column){
        return column.datatype==="time"||column.datatype==="timestamp"||column.datatype==="string"||column.datatype.startsWith("varchar");
      });
      return columns;
    };

    $scope.getColumnsByTable = function (tableName) {
        var temp = [];
        angular.forEach(TableModel.selectProjectTables, function (table) {
            if (table.name == tableName) {
                temp = table.columns;
                angular.forEach(temp,function(column){
                     column.cardinality=table.cardinality[column.name];
                });
            }
        });
        return temp;
    };
    $scope.unique = function (arr){
        var n = [];
        for(var i = 0; i < arr.length; i++) {
        		if (n.indexOf(arr[i]) == -1) {
        		    n.push(arr[i]);
        		}
        }
        return n;
    };

    $scope.iteration =function (object,parameter,cube){
        object[parameter.value]=object[parameter.value]||[];
        object[parameter.value].push(cube);
        if(parameter.next_parameter==null){
            return;
        }else{
            $scope.iteration(object,parameter.next_parameter,cube);
        }
     };

    $scope.getColumnType = function (_column,table){
        var columns = $scope.getColumnsByTable(table);
        var type;
        angular.forEach(columns,function(column){
            if(_column === column.name){
                type = column.datatype;
                return;
            }
        });
        return type;
    };


    $scope.isFormatEditable = false;
    var judgeFormatEditable = function(dateColumn){
    if(dateColumn == null){
      $scope.isFormatEditable = false;
      return;
    }
    var column = _.filter($scope.getColumnsByTable($scope.modelsManager.selectedModel.fact_table),function(_column){
      var columnName=$scope.modelsManager.selectedModel.fact_table+"."+_column.name;
      if(dateColumn == columnName)
        return _column;
    });


      var data_type = column[0].datatype;
      if(data_type ==="bigint" ||data_type ==="int" ||data_type ==="integer"){
        $scope.isFormatEditable = false;
        $scope.modelsManager.selectedModel.partition_desc.partition_date_format='yyyyMMdd';
        $scope.partitionColumn.hasSeparateTimeColumn=false;
        $scope.modelsManager.selectedModel.partition_desc.partition_time_column=null;
        $scope.modelsManager.selectedModel.partition_desc.partition_time_format=null;
        return;
      }

    $scope.isFormatEditable = true;
  };
    $scope.partitionChange = function (dateColumn) {
      judgeFormatEditable(dateColumn);
    };


    // ~ Define data
    $scope.state = {
        "modelSchema": "",
        mode:'edit',
        modelName: $scope.routeParams.modelName,
        project:$scope.projectModel.selectedProject
    };

    $scope.partitionColumn ={
      "hasSeparateTimeColumn" : false
    }
    // ~ init
    if ($scope.isEdit = !!$routeParams.modelName) {

        var modelName = $routeParams.modelName;
        ModelDescService.query({model_name: modelName}, function (model) {
          if (model) {
            $scope.modelCopy = angular.copy(model);
            modelsManager.setSelectedModel($scope.modelCopy);
            modelsEdit.setSelectedModel(model);
            $scope.lookupLength=modelsEdit.selectedModel.lookups.length;
            CubeService.list({modelName:model.name}, function (_cubes) {
              $scope.cubesLength = _cubes.length;
              angular.forEach(_cubes,function(cube){
                CubeDescService.query({cube_name:cube.name},{},function(each){
                  for(var k=0;k<each[0].dimensions.length;k++){
                    if(typeof $scope.usedDimensionsCubeMap[each[0].dimensions[k].table]!='object'){
                      $scope.usedDimensionsCubeMap[each[0].dimensions[k].table]={};
                    }
                  }
                  for(var j=0;j<each[0].dimensions.length;j++){
                    angular.forEach($scope.usedDimensionsCubeMap,function(table,tableName){
                      if(each[0].dimensions[j].table==tableName){
                        if(each[0].dimensions[j].derived!=null && each[0].dimensions[j].derived.length>0){
                          angular.forEach(each[0].dimensions[j].derived,function(derived){
                            $scope.usedDimensionsCubeMap[tableName][derived]=$scope.usedDimensionsCubeMap[tableName][derived]||[];
                            $scope.usedDimensionsCubeMap[tableName][derived].push(cube.name);

                          });
                        }
                        else{
                          $scope.usedDimensionsCubeMap[tableName][each[0].dimensions[j].column]= $scope.usedDimensionsCubeMap[tableName][each[0].dimensions[j].column]||[];
                          $scope.usedDimensionsCubeMap[tableName][each[0].dimensions[j].column].push(cube.name);
                        }
                      }
                    });
                  }
                  for(var i=0;i<each[0].measures.length;i++){
                    if(each[0].measures[i].function.parameter.type=="column"){
                      $scope.iteration($scope.usedMeasuresCubeMap,each[0].measures[i].function.parameter,cube.name);
                    }
                  }
                });
              });
            }).$promise
            if(!$scope.modelsManager.selectedModel.partition_desc.partition_data_format){
              $scope.isBigInt = true;
            }
            if($scope.modelsManager.selectedModel.partition_desc.partition_time_column){
              $scope.partitionColumn.hasSeparateTimeColumn = true;
            }
            modelsManager.selectedModel.project = ProjectModel.getProjectByCubeModel(modelName);
            if(!ProjectModel.getSelectedProject()){
              ProjectModel.setSelectedProject(modelsManager.selectedModel.project);
            }

            TableModel.aceSrcTbLoaded().then(function(){
              judgeFormatEditable($scope.modelsManager.selectedModel.partition_desc.partition_date_column);
            });

          }
        });
        //init project

    } else {
        MetaModel.initModel();
        modelsManager.selectedModel = MetaModel.getMetaModel();
        modelsManager.selectedModel.project = ProjectModel.getSelectedProject();
    }

  $scope.toggleHasSeparateColumn = function(hasSeparateTimeColumn){
    if(!hasSeparateTimeColumn){
      $scope.modelsManager.selectedModel.partition_desc.partition_time_column = null;
    }
  }

    $scope.prepareModel = function () {
        // generate column family
        $scope.state.project = modelsManager.selectedModel.project;
        var _model = angular.copy(modelsManager.selectedModel);
        delete _model.project;
        $scope.state.modelSchema = angular.toJson(_model, true);

    };

    $scope.modelResultTmpl = function (notification) {
        // Get the static notification template.
        var tmpl = notification.type == 'success' ? 'modelResultSuccess.html' : 'modelResultError.html';
        return $interpolate($templateCache.get(tmpl))(notification);
    };

    $scope.saveModel = function () {

        $scope.prepareModel();

        try {
            angular.fromJson($scope.state.modelSchema);
        } catch (e) {
            SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_invalid_model_json, 'error');
            return;
        }

        SweetAlert.swal({
            title: $scope.isEdit?$scope.dataKylin.alert.tip_to_update_model:$scope.dataKylin.alert.tip_to_save_model,
            text: $scope.isEdit?$scope.dataKylin.alert.tip_Please_note:'',
            type: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: true
        }, function(isConfirm) {
            if(isConfirm){
                loadingRequest.show();

                if ($scope.isEdit) {
                    ModelService.update({}, {
                      modelDescData:VdmUtil.filterNullValInObj($scope.state.modelSchema),
                      modelName: $routeParams.modelName,
                      project: $scope.state.project
                    }, function (request) {
                        if (request.successful) {
                            $scope.state.modelSchema = request.modelSchema;
                            SweetAlert.swal('', $scope.dataKylin.alert.success_updated_model, 'success');
                            $location.path("/models");
                            //location.reload();
                        } else {
                               $scope.saveModelRollBack();
                                var message =request.message;
                                var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
                                MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
                        $scope.saveModelRollBack();

                        if(e.data&& e.data.exception){
                            var message =e.data.exception;
                            var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;

                            MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        } else {
                            MessageService.sendMsg($scope.modelResultTmpl({'text':$scope.dataKylin.alert.error_info,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }
                        loadingRequest.hide();
                    });
                } else {
                    ModelService.save({}, {
                      modelDescData:VdmUtil.filterNullValInObj($scope.state.modelSchema),
                      project: $scope.state.project
                    }, function (request) {
                        if(request.successful) {

                          $scope.state.modelSchema = request.modelSchema;
                          SweetAlert.swal('', $scope.dataKylin.alert.success_created_model, 'success');
                          $location.path("/models");
                          //location.reload();
                        } else {
                            $scope.saveModelRollBack();
                            var message =request.message;
                            var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
                            MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }

                        //end loading
                        loadingRequest.hide();
                    }, function (e) {
                        $scope.saveModelRollBack();

                        if (e.data && e.data.exception) {
                            var message =e.data.exception;
                            var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
                            MessageService.sendMsg($scope.modelResultTmpl({'text':msg,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        } else {
                            MessageService.sendMsg($scope.modelResultTmpl({'text':$scope.dataKylin.alert.error_info,'schema':$scope.state.modelSchema}), 'error', {}, true, 'top_center');
                        }
                        //end loading
                        loadingRequest.hide();

                    });
                }
            }
            else{
                $scope.saveModelRollBack();
            }
        });
    };

//    reverse the date
    $scope.saveModelRollBack = function (){
    };

    $scope.removeTableDimensions = function(tableIndex){
        modelsManager.selectedModel.dimensions.splice(tableIndex,1);
    }
    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
      if(!$scope.projectModel.getSelectedProject()) {
        return;
      }
      var param = {
            ext: true,
            project:newValue
        };
        if(newValue){
            TableModel.initTables();
            TableService.list(param, function (tables) {
                angular.forEach(tables, function (table) {
                    table.name = table.database+"."+table.name;
                    TableModel.addTable(table);
                });
            });
        }
    });
});
