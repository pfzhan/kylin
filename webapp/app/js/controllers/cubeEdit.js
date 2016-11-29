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


KylinApp.controller('CubeEditCtrl', function ($scope,$rootScope, $q, $routeParams, $location, $templateCache, $interpolate, MessageService, TableService, CubeDescService, CubeService,RawTablesService, loadingRequest, SweetAlert, $log, cubeConfig, CubeDescModel, MetaModel, TableModel, ModelDescService, modelsManager, cubesManager, ProjectModel, StreamingModel, StreamingService,kylinCommon,VdmUtil) {
  $scope.cubeConfig = cubeConfig;
  $scope.initMeasures={statu:false};
  $scope.cubeName=$routeParams.cubeName;
  $scope.metaModel = {};
  $scope.modelsManager = modelsManager;
  TableModel.aceSrcTbLoaded();

  //add or edit ?
  var absUrl = $location.absUrl();
  $scope.cubeMode = absUrl.indexOf("/cubes/add") != -1 ? 'addNewCube' : absUrl.indexOf("/cubes/edit") != -1 ? 'editExistCube' : 'default';

  if ($scope.cubeMode == "addNewCube" &&ProjectModel.selectedProject==null) {
    SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_project, 'warning');
    $location.path("/models");
    return;
  }

  //init encoding list
  $scope.store = {
    supportedEncoding:[]
  }
  CubeService.getValidEncodings({}, function (encodings) {
    if(encodings){
      delete encodings.$promise;
      delete encodings.$resolved;
      for(var i in encodings)
        if(encodings.hasOwnProperty(i)){
          var value = i
          var name = value;
          var typeVersion=+encodings[i];
          if(value=="int"){
            name = "int (deprecated)";
          }
          if(/\d+/.test(""+typeVersion)&&typeVersion>1){
            for(var s=1;s<=typeVersion;s++){
              $scope.store.supportedEncoding.push({
                "name":name+" (v"+s+","+(s==typeVersion?"suggest)":")"),
                "value":value+"[v"+s+"]",
                "version":typeVersion,
                "baseValue":value,
                "suggest":s==typeVersion

              });
            }
          }else {
            $scope.store.supportedEncoding.push({
              "name": name,
              "value": value+"[v1]",
              "encoding_version":1,
              "version":typeVersion,
              "baseValue":value,
              "suggest":true
            });
          }
        }
    }
  },function(e){
    $scope.store.supportedEncoding = $scope.cubeConfig.encodings;
  })
  $scope.createFilter=function(type){
    if(type.indexOf("varchar")<=0){
      return ['fixed_length_hex'];
    }else if(type!="date"){
      return ['date'];
    }else if(type!="time"&&type!="datetime"&&type!="timestamp"){
      return ['time'];
    }else{
      return [];
    }
  }
  $scope.getEncodings =function (name){
    var filterName=name;
    var type = TableModel.columnNameTypeMap[filterName]||'';
    var encodings =$scope.store.supportedEncoding,filterEncoding;
    var filerList=$scope.createFilter(type);
    if($scope.isEdit){
      if($scope.cubeMetaFrame.rowkey.rowkey_columns&&name){
        for(var s=0;s<$scope.cubeMetaFrame.rowkey.rowkey_columns.length;s++){
          if(filterName==$scope.cubeMetaFrame.rowkey.rowkey_columns[s].column){
            var version=$scope.cubeMetaFrame.rowkey.rowkey_columns[s].encoding_version;
            filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'value',$scope.cubeMetaFrame.rowkey.rowkey_columns[s].encoding.replace(/:\d+/,"")+(version?"[v"+version+"]":"[v1]"),'suggest',true)
          }
        }
      }else{
        filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
      }
    }else{
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
    }
    for(var f=0;f<filerList.length;f++){
      filterEncoding=VdmUtil.removeFilterObjectList(filterEncoding,'baseValue',filerList[f]);
    }
    return filterEncoding;
  }




  $scope.getColumnsByTable = function (tableName) {
    var temp = [];
    angular.forEach(TableModel.selectProjectTables, function (table) {
      if (table.name == tableName) {
        temp = table.columns;
      }
    });
    return temp;
  };

  //get columns from model
  $scope.getDimColumnsByTable = function (tableName) {
    if (!tableName) {
      return [];
    }
    var tableColumns = $scope.getColumnsByTable(tableName);
    var tableDim = _.find($scope.metaModel.model.dimensions, function (dimension) {
      return dimension.table == tableName
    });
    if(!tableDim){
      return [];
    }
    var tableDimColumns = tableDim.columns;
    var avaColObject = _.filter(tableColumns, function (col) {
      return tableDimColumns.indexOf(col.name) != -1;
    });
    return avaColObject;
  };

  $scope.getMetricColumnsByTable = function (tableName) {
    if (!tableName) {
      return [];
    }
    var tableColumns = $scope.getColumnsByTable(tableName);
    var tableMetrics = $scope.metaModel.model.metrics;
    var avaColObject = _.filter(tableColumns, function (col) {
      return tableMetrics.indexOf(col.name) != -1;
    });
    return avaColObject;
  };

  $scope.getCommonMetricColumns = function () {
    //metric from model
    var me_columns = [];
    if($scope.metaModel.model.metrics){
      angular.forEach($scope.metaModel.model.metrics,function(metric,index){
        me_columns.push(metric);
      })
    }

    return me_columns;
  };

  $scope.getAllModelDimMeasureColumns = function () {
    var me_columns = [];
    if($scope.metaModel.model.metrics){
      angular.forEach($scope.metaModel.model.metrics,function(metric,index){
        me_columns.push(metric);
      })
    }

    angular.forEach($scope.metaModel.model.dimensions,function(dimension,index){
      if(dimension.columns){
        me_columns = me_columns.concat(dimension.columns);
      }
    })

    return distinct_array(me_columns);
  };

  $scope.getAllModelDimColumns = function () {
    var me_columns = [];
    angular.forEach($scope.metaModel.model.dimensions,function(dimension,index){
      if(dimension.columns){
        me_columns = me_columns.concat(dimension.columns);
      }
    })

    return distinct_array(me_columns);
  };

  function distinct_array(arrays){
    var arr = [];
    for(var item in arrays){
      if(arr.indexOf(arrays[item])==-1){
        arr.push(arrays[item]);
      }
    }
    return arr;
  }


  $scope.getExtendedHostColumn = function(){
    var me_columns = [];
    //add cube dimension column for specific measure
    angular.forEach($scope.cubeMetaFrame.dimensions,function(dimension,index){
      if($scope.metaModel.model.fact_table !== dimension.table){
        return;
      }
      if(dimension.column && dimension.derived == null){
        me_columns.push(dimension.column);
      }
    });
    return me_columns;
  }


  $scope.getFactColumns = function () {
    var me_columns = [];
    angular.forEach($scope.cubeMetaFrame.dimensions,function(dimension,index){
      if(dimension.column && dimension.derived == null){
        me_columns.push(dimension.column);
      }
      else{
        angular.forEach(dimension.derived,function(derived){
          me_columns.push(derived);
        });
      }
    });
    angular.forEach($scope.cubeMetaFrame.measure,function(measure){
      if(measure.function.parameter.type=="column"){
        me_columns.push(measure.function.parameter.value);
      }
    });
    var unique = []
    angular.forEach(me_columns, function (column) {
      if (unique.indexOf(column) === -1) {
        unique.push(column);
      }
    });
    return unique;
  };

  $scope.getColumnType = function (_column, table) {
    var columns = $scope.getColumnsByTable(table);
    var type;
    angular.forEach(columns, function (column) {
      if (_column === column.name) {
        type = column.datatype;
        return;
      }
    });
    return type;
  };

  var ColFamily = function () {
    var index = 1;
    return function () {
      var newColFamily =
      {
        "name": "f" + index,
        "columns": [
          {
            "qualifier": "m",
            "measure_refs": []
          }
        ]
      };
      index += 1;

      return newColFamily;
    }
  };


  // ~ Define data
  $scope.state = {
    "cubeSchema": "",
    "cubeInstance":"",
    "mode": 'edit'
  };

  $scope.cubeState={
    "isStreaming": false
  }

  //fetch cube info and model info in edit model
  // ~ init
  if ($scope.isEdit = !!$routeParams.cubeName) {

    CubeDescService.query({cube_name: $routeParams.cubeName}, function (detail) {
      if (detail.length > 0) {
        $scope.cubeMetaFrame = detail[0];
        $scope.metaModel = {};

        //get model from API when page refresh
        if (!modelsManager.getModels().length) {
          ModelDescService.query({model_name: $scope.cubeMetaFrame.model_name}, function (_model) {
            $scope.metaModel.model = _model;
          });
        }

        $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);


      }
    });

    var queryParam = {
          cubeId: $routeParams.cubeName
        };
      CubeService.getCube(queryParam, {},function(instance){
          if (instance) {
              $scope.instance = instance;
              $scope.state.cubeInstance =angular.toJson($scope.instance,true);

              } else {
              SweetAlert.swal($scope.dataKylin.alert.oops,$scope.dataKylin.alert.error_cube_edit_cube_detail, 'error');
            }

          },function(e){
          if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : $scope.dataKylin.alert.check_cube_edit_query_param;
              SweetAlert.swal($scope.dataKylin.alert.oops, msg, 'error');
            } else {
              SweetAlert.swal($scope.dataKylin.alert.oops,$scope.dataKylin.alert.check_cube_edit_query_param, 'error');
            }
        });




} else {

    $scope.cubeMetaFrame = CubeDescModel.createNew();
    $scope.metaModel = {
      model: modelsManager.getModel($scope.cubeMetaFrame.model_name)
    }

    $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);
  }


  $scope.prepareCube = function () {
    //generate column family
    generateColumnFamily();
    //generate rowkey
    reGenerateRowKey();

    if ($scope.metaModel.model.partition_desc.partition_date_column && ($scope.cubeMetaFrame.partition_date_start | $scope.cubeMetaFrame.partition_date_start == 0)) {

      if ($scope.metaModel.model.partition_desc.partition_date_column.indexOf(".") == -1) {
        $scope.metaModel.model.partition_desc.partition_date_column = $scope.metaModel.model.fact_table + "." + $scope.metaModel.model.partition_desc.partition_date_column;
      }

    }

    //set model ref for cubeDesc
    if ($scope.cubeMetaFrame.model_name === "" || angular.isUndefined($scope.cubeMetaFrame.model_name)) {
      $scope.cubeMetaFrame.model_name = $scope.cubeMetaFrame.name;
    }

    $scope.state.project = ProjectModel.getSelectedProject();
//        delete $scope.cubeMetaFrame.project;


    $scope.state.cubeSchema = angular.toJson($scope.cubeMetaFrame, true);

  };

  $scope.cubeResultTmpl = function (notification) {
    // Get the static notification template.
    var tmpl = notification.type == 'success' ? 'cubeResultSuccess.html' : 'cubeResultError.html';
    return $interpolate($templateCache.get(tmpl))(notification);
  };




  $scope.saveCube = function () {

    try {
      angular.fromJson($scope.state.cubeSchema);
    } catch (e) {
      SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_invalid_cube, 'error');
      return;
    }

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_save_cube,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function (isConfirm) {
      if (isConfirm) {
        loadingRequest.show();
        if ($scope.isEdit) {
          CubeService.update({}, {
            cubeDescData:VdmUtil.filterNullValInObj($scope.state.cubeSchema),
            cubeName: $routeParams.cubeName,
            project: $scope.state.project
          }, function (request) {
            if (request.successful) {
              VdmUtil.storage.remove($scope.state.project);
              $scope.state.cubeSchema = request.cubeDescData;
              kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_updated_cube);
              //更新rawTable

              if($scope.RawTables&&$scope.RawTables.columns){
                if($scope.RawTables.needAdd){
                  saveRawTable(function(){
                    $location.path("/models");
                  },function(){
                    $location.path("/models");
                  });
                  return;
                }
                updateRawTable(function(){
                  $location.path("/models");
                },function(){
                  $location.path("/models");
                });
              }else if($scope.RawTables&&$scope.RawTables.needDelete){
                RawTablesService.delete({rawTableName:$scope.cubeMetaFrame.name},{},function(){
                  $location.path("/models");
                },function(e){
                  rawTableSaveError(e);
                  $location.path("/models");
                })

              }else{
                $location.path("/models");
              }

            } else {
              $scope.saveCubeRollBack();
              $scope.cubeMetaFrame.project = $scope.state.project;
              var message = request.message;
              var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }
            //end loading
            loadingRequest.hide();
          }, function (e) {
            $scope.saveCubeRollBack();

            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            } else {
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': $scope.dataKylin.alert.error_info,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }
            loadingRequest.hide();
          });



        } else {
          //保存cube
          CubeService.save({}, {
            cubeDescData: VdmUtil.filterNullValInObj($scope.state.cubeSchema),
            project: $scope.state.project
          }, function (request) {
            if (request.successful) {
              VdmUtil.storage.remove($scope.state.project);
              $scope.state.cubeSchema = request.cubeDescData;
              kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_created_cube);

              //location.reload();
              saveRawTable(function(){
                $location.path("/models");
              },function(){
                $location.path("/models");
              });
            } else {
              $scope.saveCubeRollBack();
              $scope.cubeMetaFrame.project = $scope.state.project;
              var message = request.message;
              var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }

            //end loading
            loadingRequest.hide();
          }, function (e) {
            $scope.saveCubeRollBack();

            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
              MessageService.sendMsg($scope.cubeResultTmpl({
                'text': msg,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            } else {
              MessageService.sendMsg($scope.cubeResultTmpl  ({
                'text': $scope.dataKylin.alert.error_info,
                'schema': $scope.state.cubeSchema
              }), 'error', {}, true, 'top_center');
            }
            //end loading
            loadingRequest.hide();

          });




        }

      }
      else {
        $scope.saveCubeRollBack();
      }
    });
  };
  $scope.getTypeVersion=function(typename){
    var searchResult=/\[v(\d+)\]/.exec(typename);
    if(searchResult&&searchResult.length){
      return searchResult.length&&searchResult[1]||1;
    }else{
      return 1;
    }
  }
  $scope.removeVersion=function(typename){
    if(typename){
      return typename.replace(/\[v\d+\]/g,"");
    }
    return "";
  }

  $scope.changeRawTableDataFromClient=function(rawtableData){
    var data=[].concat(rawtableData);
    var needLengthKeyList=['fixed_length','fixed_length_hex','int','integer'];
    var len=data&&data.length|| 0;
    for(var i=0;i<len;i++){
      var version= $scope.getTypeVersion(data[i].encoding);
      var baseKey = $scope.removeVersion(data[i].encoding).replace(/:\[\d+\]/,'');
      if(needLengthKeyList.indexOf(baseKey)>=0){
        data[i].encoding=baseKey+':'+data[i].valueLength;
      }else{
        data[i].encoding=baseKey;
      }
      data[i].encoding_version=version;
    }
    return data;
  }

  //保存rawTable
  function saveRawTable(successCallback,errorCallback){
    if($scope.RawTables&&$scope.RawTables.columns){
      var rawTableConfig=angular.copy($scope.RawTables);
      rawTableConfig.name=$scope.cubeMetaFrame.name;
      rawTableConfig.model_name=$scope.cubeMetaFrame.model_name;
      rawTableConfig.engine_type=$scope.cubeMetaFrame.engine_type;
      rawTableConfig.storage_type=$scope.cubeMetaFrame.storage_type;

      rawTableConfig.columns=$scope.changeRawTableDataFromClient($scope.RawTables.columns)
      RawTablesService.save({},{
        rawTableDescData:VdmUtil.filterNullValInObj(rawTableConfig),
        project: $scope.state.project
      },function(request){
        VdmUtil.storage.remove($scope.state.project+"_rawtable");
        loadingRequest.hide();
        if(typeof successCallback=='function'){
          successCallback();
        }
      },function(e){
        rawTableSaveError(e)
        loadingRequest.hide();
        if(typeof errorCallback=='function'){
          errorCallback();
        }
      })
    }else{
      if(typeof successCallback=='function'){
        successCallback();
      }
    }
  }

  //更新rawTable
  function updateRawTable(successCallback,errorCallback){
    var rawTableConfig=angular.copy($scope.RawTables);
    rawTableConfig.columns=$scope.changeRawTableDataFromClient($scope.RawTables.columns)
    //rawTableConfig.column=$scope.changeRawTableDataFromClient($scope.RawTables.columns)
    RawTablesService.update({},{
      rawTableDescData:VdmUtil.filterNullValInObj(rawTableConfig),
      project: $scope.state.project,
      rawTableName:$scope.RawTables.name
    },function(request){
      VdmUtil.storage.remove($scope.state.project+"_rawtable");
      loadingRequest.hide();
      if(typeof successCallback=='function'){
        successCallback();
      }
    },function(e){
      rawTableSaveError(e)
      loadingRequest.hide();
      if(typeof errorCallback=='function'){
        errorCallback();
      }
    })
  }

  function rawTableSaveError(e){
    if (e&&e.data && e.data.exception) {
      var message = e.data.exception;
      var msg = !!(message) ? message : $scope.dataKylin.alert.rawTableSaveError;
      MessageService.sendMsg(msg, 'error', {});
    } else {
      MessageService.sendMsg($scope.dataKylin.alert.rawTableSaveError, 'error',{});
    }
  }



//    reverse the date
  $scope.saveCubeRollBack = function () {
  }

  $scope.updateMandatory = function (rowkey_column) {
    if (!rowkey_column.mandatory) {
      angular.forEach($scope.cubeMetaFrame.aggregation_groups, function (group, index) {
        var index = group.indexOf(rowkey_column.column);
        if (index > -1) {
          group.splice(index, 1);
        }
      });
    }
  }

  function reGenerateRowKey() {
    var tmpRowKeyColumns = [];
    var tmpAggregationItems = [];//put all aggregation item
    //var hierarchyItemArray = [];//put all hierarchy items


    var pfkMap = {};

    for( var i=0;i<$scope.metaModel.model.lookups.length;i++){
      var lookup = $scope.metaModel.model.lookups[i];
      var table = lookup.table;
      pfkMap[table] = {};
      for(var j=0;j<lookup.join.primary_key.length;j++){
        var pk = lookup.join.primary_key[j];
        pfkMap[table][pk] = lookup.join.foreign_key[j];
      }

    }


    angular.forEach($scope.cubeMetaFrame.dimensions, function (dimension, index) {

      //derived column
      if (dimension.derived && dimension.derived.length) {
        var lookup = _.find($scope.metaModel.model.lookups, function (lookup) {
          return lookup.table == dimension.table
        });
        angular.forEach(lookup.join.foreign_key, function (fk, index) {
          for (var i = 0; i < tmpRowKeyColumns.length; i++) {
            if (VdmUtil.removeNameSpace(tmpRowKeyColumns[i].column) == VdmUtil.removeNameSpace(fk))
              break;
          }
          // push to array if no duplicate value
          if (i == tmpRowKeyColumns.length) {
            tmpRowKeyColumns.push({
              "column": VdmUtil.removeNameSpace(tableName)+'.'+VdmUtil.removeNameSpace(fk),
              "encoding": "dict",
              "isShardBy": "false"
            });

            tmpAggregationItems.push(fk);
          }
        })

      }
      //normal column
      else if (dimension.column  && !dimension.derived) {

        var tableName = dimension.table;
        var columnName = dimension.column;
        var rowkeyColumn = dimension.column;
        if(pfkMap[tableName]&&pfkMap[tableName][columnName]){
          //lookup table primary key column as dimension
          rowkeyColumn = pfkMap[tableName][columnName];
        }


        for (var i = 0; i < tmpRowKeyColumns.length; i++) {
          if (VdmUtil.removeNameSpace(tmpRowKeyColumns[i].column) == VdmUtil.removeNameSpace(rowkeyColumn))
            break;
        }
        if (i == tmpRowKeyColumns.length) {
          tmpRowKeyColumns.push({
            "column": VdmUtil.removeNameSpace(tableName)+'.'+VdmUtil.removeNameSpace(rowkeyColumn),
            "encoding": "dict",
            "isShardBy": "false"
          });
          tmpAggregationItems.push(rowkeyColumn);
        }
      }

    });

    var rowkeyColumns = $scope.cubeMetaFrame.rowkey.rowkey_columns;
    var newRowKeyColumns = sortSharedData(rowkeyColumns, tmpRowKeyColumns);
    var increasedColumns = increasedColumn(rowkeyColumns, tmpRowKeyColumns);
    newRowKeyColumns = newRowKeyColumns.concat(increasedColumns);

    //! here get the latest rowkey_columns
    $scope.cubeMetaFrame.rowkey.rowkey_columns = newRowKeyColumns;

    if ($scope.cubeMode === "editExistCube") {
      //clear dims will not be used
      var aggregationGroups = $scope.cubeMetaFrame.aggregation_groups;
      rmDeprecatedDims(aggregationGroups,tmpAggregationItems);
    }

    if ($scope.cubeMode === "addNewCube") {

      //clear dims will not be used
      if($scope.cubeMetaFrame.aggregation_groups.length){
        var aggregationGroups = $scope.cubeMetaFrame.aggregation_groups;
        rmDeprecatedDims(aggregationGroups,tmpAggregationItems);
        return;
      }


      if (!tmpAggregationItems.length) {
        $scope.cubeMetaFrame.aggregation_groups = [];
        return;
      }

      var newUniqAggregationItem = [];
      angular.forEach(tmpAggregationItems, function (item, index) {
        if (newUniqAggregationItem.indexOf(item) == -1) {
          newUniqAggregationItem.push(item);
        }
      });

      $scope.cubeMetaFrame.aggregation_groups = [];
      var initJointGroups = sliceGroupItemToGroups(newUniqAggregationItem);
      var newGroup =  CubeDescModel.createAggGroup();
      newGroup.includes = newUniqAggregationItem;
      for(var i=1;i<initJointGroups.length;i++){
        if(initJointGroups[i].length>1){
          newGroup.select_rule.joint_dims[i-1] = initJointGroups[i];
        }
      }
      $scope.cubeMetaFrame.aggregation_groups.push(newGroup);

    }
  }

  function rmDeprecatedDims(aggregationGroups,tmpAggregationItems){
    angular.forEach(aggregationGroups, function (group, index) {
      if (group) {
        for (var j = 0; j < group.includes.length; j++) {
          var elemStillExist = false;
          for (var k = 0; k < tmpAggregationItems.length; k++) {
            if (VdmUtil.removeNameSpace(group.includes[j].toUpperCase()) == VdmUtil.removeNameSpace(tmpAggregationItems[k].toUpperCase())) {
              elemStillExist = true;
              break;
            }
          }
          if (!elemStillExist) {
            var deprecatedItem = group.includes[j];
            //rm deprecated dimension from include
            group.includes.splice(j, 1);
            j--;

            //rm deprecated dimension in mandatory dimensions
            var mandatory = group.select_rule.mandatory_dims;
            if(mandatory && mandatory.length){
              var columnIndex = mandatory.indexOf(deprecatedItem);
              group.select_rule.mandatory_dims.splice(columnIndex,1);
            }

            var hierarchys =  group.select_rule.hierarchy_dims;
            if(hierarchys && hierarchys.length){
              for(var i=0;i<hierarchys.length;i++){
                var hierarchysIndex = hierarchys[i].indexOf(deprecatedItem);
                group.select_rule.hierarchy_dims[i].splice(hierarchysIndex,1);
              }

            }

            var joints =  group.select_rule.joint_dims;
            if(joints && joints.length){
              for(var i=0;i<joints.length;i++){
                var jointIndex = joints[i].indexOf(deprecatedItem);
                group.select_rule.joint_dims[i].splice(jointIndex,1);
              }

            }

          }
        }
      }
      else {
        aggregationGroups.splice(index, 1);
        index--;
      }
    });
  }

  function sortSharedData(oldArray, tmpArr) {
    var newArr = [];
    for (var j = 0; j < oldArray.length; j++) {
      var unit = oldArray[j];
      for (var k = 0; k < tmpArr.length; k++) {
        if (VdmUtil.removeNameSpace(unit.column)==VdmUtil.removeNameSpace(tmpArr[k].column)) {
          newArr.push(unit);
        }
      }
    }
    return newArr;
  }

  function increasedData(oldArray, tmpArr) {
    var increasedData = [];
    if (oldArray && !oldArray.length) {
      return increasedData.concat(tmpArr);
    }

    for (var j = 0; j < tmpArr.length; j++) {
      var unit = tmpArr[j];
      var exist = false;
      for (var k = 0; k < oldArray.length; k++) {
        if (unit == oldArray[k]) {
          exist = true;
          break;
        }
      }
      if (!exist) {
        increasedData.push(unit);
      }
    }
    return increasedData;
  }

  function increasedColumn(oldArray, tmpArr) {
    var increasedData = [];
    if (oldArray && !oldArray.length) {
      return increasedData.concat(tmpArr);
    }

    for (var j = 0; j < tmpArr.length; j++) {
      var unit = tmpArr[j];
      var exist = false;
      for (var k = 0; k < oldArray.length; k++) {
        if (unit.column == oldArray[k].column) {
          exist = true;
          break;
        }
      }
      if (!exist) {
        increasedData.push(unit);
      }
    }
    return increasedData;
  }

  function sliceGroupItemToGroups(groupItems) {
    if (!groupItems.length) {
      return [];
    }
    var groups = [];
    var j = -1;
    for (var i = 0; i < groupItems.length; i++) {
      if (i % 11 == 0) {
        j++;
        groups[j] = [];
      }
      groups[j].push(groupItems[i]);
    }
    return groups;
  }


  // ~ private methods
  function generateColumnFamily() {
    $scope.cubeMetaFrame.hbase_mapping.column_family = [];
    var colFamily = ColFamily();
    var normalMeasures = [], distinctCountMeasures = [];
    angular.forEach($scope.cubeMetaFrame.measures, function (measure, index) {
      if (measure.function.expression === 'COUNT_DISTINCT') {
        distinctCountMeasures.push(measure);
      } else {
        normalMeasures.push(measure);
      }
    });
    if (normalMeasures.length > 0) {
      var nmcf = colFamily();
      angular.forEach(normalMeasures, function (normalM, index) {
        nmcf.columns[0].measure_refs.push(normalM.name);
      });
      $scope.cubeMetaFrame.hbase_mapping.column_family.push(nmcf);
    }

    if (distinctCountMeasures.length > 0) {
      var dccf = colFamily();
      angular.forEach(distinctCountMeasures, function (dcm, index) {
        dccf.columns[0].measure_refs.push(dcm.name);
      });
      $scope.cubeMetaFrame.hbase_mapping.column_family.push(dccf);
    }
  }

  $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
    if(!$scope.projectModel.getSelectedProject()) {
      return;
    }
    var param = {
      ext: true,
      project: newValue
    };
    if (newValue) {
      TableModel.initTables();
      TableService.list(param, function (tables) {
        angular.forEach(tables, function (table) {
          table.name = table.database + "." + table.name;
          TableModel.tableColumnMap[table.name]={};
          angular.forEach(table.columns, function (column) {
            TableModel.tableColumnMap[table.name][column.name]={
            name:column.name,
            datatype:column.datatype,
            cardinality:table.cardinality[column.name],
            comment:column.comment};
          });

          TableModel.addTable(table);
        });
      });
    }
  });

  $scope.$watch('cubeMetaFrame.model_name', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    $scope.metaModel.model = modelsManager.getModel(newValue);
    if(!$scope.metaModel.model){
      return;
    }
  });

  $scope.removeNotificationEvents = function(){
    if($scope.cubeMetaFrame.status_need_notify.indexOf('ERROR') == -1){
      $scope.cubeMetaFrame.status_need_notify.unshift('ERROR');
    }
  }

  $scope.$on('DimensionsEdited', function (event) {
    if ($scope.cubeMetaFrame) {
      reGenerateRowKey();
    }
  });


  if(!ProjectModel.getSelectedProject()&&!$scope.isEdit){
    $location.path("/models/fromadd");
  }
  //$scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
  //  if (newValue != oldValue || newValue == null) {
  //    modelsManager.removeAll();
  //  }
  //});
  $scope.hisRawTableData;
  if($scope.isEdit) {
    RawTablesService.getRawTableInfo({rawTableName: $routeParams.cubeName}, {}, function (request) {
      if (request && request.columns && request.columns.length) {
        $scope.hisRawTableData = request.columns;
      }
    })
  }
  //RawTables数据变化
  $scope.RawTables;
  $scope.$on('RawTableEdited', function (event,data) {
    $scope.RawTables=data;
    VdmUtil.storage.setObject(ProjectModel.getSelectedProject()+"_rawtable",data);
  });
  $scope.$watch("cubeMetaFrame",function(){
    if(ProjectModel.getSelectedProject()&&$scope.cubeMetaFrame!=""){
      VdmUtil.storage.setObject(ProjectModel.getSelectedProject(),$scope.cubeMetaFrame);
    }
  },true);

  var proName=ProjectModel.getSelectedProject();
  if($scope.cubeMetaFrame&&$scope.cubeMetaFrame.name==""&&VdmUtil.storage.getObject(ProjectModel.getSelectedProject()).name){
    $scope.cubeMetaFrame=VdmUtil.storage.getObject(proName);
    $scope.RawTables=VdmUtil.storage.getObject(proName+"_rawtable")||[];
   }
});
