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

KylinApp.controller('CubeMeasuresCtrl', function ($scope, $modal,TableModel,MetaModel,cubesManager,CubeDescModel,SweetAlert,kylinCommon,VdmUtil,StringHelper) {
  $scope.TableModel=TableModel;
  $scope.num=0;
  $scope.convertedColumns=[];
  $scope.groupby=[];
  if ($scope.state.mode =="edit"&&($scope.isEdit = !$scope.cubeName)&&$scope.$parent.initMeasures.statu==false) {
    if(!$scope.cubeMetaFrame.measures||$scope.cubeMetaFrame.measures&&$scope.cubeMetaFrame.measures.length<=1){
      CubeDescModel.initMeasures($scope.cubeMetaFrame.measures,$scope.metaModel.model);
      $scope.$parent.initMeasures.statu=true;
    }
  }
  $scope.initUpdateMeasureStatus = function(){
    $scope.updateMeasureStatus = {
      isEdit:false,
      editIndex:-1
    }
  };
  $scope.initUpdateMeasureStatus();
  var needLengthKeyList=['fixed_length','fixed_length_hex','int','integer'];
  $scope.removeVersion=function(typename){
    if(typename){
      return typename.replace(/\[v\d+\]/g,"");
    }
    return "";
  }
  $scope.getTypeVersion=function(typename){
    var searchResult=/\[v(\d+)\]/.exec(typename);
    if(searchResult&&searchResult.length){
      return searchResult.length&&searchResult[1]||1;
    }else{
      return 1;
    }
  }
  $scope.createFilter=function(type){
    var matchlist=[];
    if(type.indexOf("varchar")<0){
      matchlist.push('fixed_length_hex');
      matchlist.push('fixed_length');
    }
    return matchlist;
  }
  $scope.getEncodings =function (name){
    var type = TableModel.columnNameTypeMap[name]||'';
    var encodings =$scope.store.supportedEncoding,filterEncoding=[];
    var filerList=$scope.createFilter(type);
    if($scope.isEdit) {
      if (name && $scope.newMeasure.function.configuration&&$scope.newMeasure.function.configuration['topn.encoding.' + name]) {
        var version = $scope.newMeasure.function.configuration['topn.encoding_version.' + name] || 1;
        filterEncoding = VdmUtil.getFilterObjectListByOrFilterVal(encodings, 'value', $scope.newMeasure.function.configuration['topn.encoding.' + name].replace(/:\d+/, "") + (version ? "[v" + version + "]" : "[v1]"), 'suggest', true);
      }else{
        filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest', true);
      }
    }else{
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest', true);
    }
    for(var f=0;f<filerList.length;f++){
      filterEncoding=VdmUtil.removeFilterObjectList(filterEncoding,'baseValue',filerList[f]);
    }
    return filterEncoding;
  }
  $scope.addEditMeasureEncoding = function (measure,index) {

    $scope.nextParameters = [];
    if(!!measure && measure.function.parameter.next_parameter){
      $scope.nextPara.value = measure.function.parameter.next_parameter.value;
    }
    if($scope.newMeasure.function.parameter.value){
      if($scope.metaModel.model.metrics&&$scope.metaModel.model.metrics.indexOf($scope.newMeasure.function.parameter.value)!=-1){
          $scope.newMeasure.showDim=false;
      }else{
          $scope.newMeasure.showDim=true;
      }
    }else{
      $scope.newMeasure.showDim=false;
    }
    $scope.measureParamValueUpdate();
    if(measure.function.expression=="TOP_N"){
      $scope.convertedColumns=[];
        if(measure.function.configuration==null){
          var GroupBy = {
            name:measure.function.parameter.next_parameter.value,
            encoding:"dict",
            valueLength:0,
            encodingName:"dict"
          }
        $scope.convertedColumns.push(GroupBy);
        }

      for(var configuration in measure.function.configuration) {
        if (/topn\.encoding\./.test(configuration)) {
          var _name = configuration.slice(14);
          var item = measure.function.configuration[configuration];
          var _encoding = item;
          var _valueLength = 0;
          var version = measure.function.configuration['topn.encoding_version.' + _name] || 1;
          item = $scope.removeVersion(item);
          var baseKey = item.replace(/:\d+/, '');
          if (needLengthKeyList.indexOf(baseKey) > -1) {
            var result = /:(\d+)/.exec(item);
            _valueLength = result ? result[1] : 0;
          }
          _encoding = baseKey;
          $scope.GroupBy = {
            name: _name,
            encoding: _encoding + (version ? "[v" + version + "]" : "[v1]"),
            encodingName:_encoding,
            valueLength: _valueLength,
            encoding_version: version || 1
          }
          $scope.convertedColumns.push($scope.GroupBy);
        }
      }
    }
  };


  $scope.updateNextParameter = function(){
    //jQuery.extend(true, {},$scope.newMeasure.function.parameter.next_parameter)
    for(var i= 0;i<$scope.nextParameters.length-1;i++){
      $scope.nextParameters[i].next_parameter=$scope.nextParameters[i+1];
    }
    $scope.newMeasure.function.parameter.next_parameter = $scope.nextParameters[0];
  }

  $scope.editNextParameter = function(parameter){
    $scope.openParameterModal(parameter);

  }

  $scope.addNextParameter = function(){
    $scope.openParameterModal();
  }

  $scope.removeParameter = function(parameters,index){
    if(index>-1){
      parameters.splice(index,1);
    }
    $scope.updateNextParameter();
  }


  $scope.nextPara = {
    "type":"column",
    "value":"",
    "next_parameter":null
  }

  $scope.openParameterModal = function (parameter) {
    $modal.open({
      templateUrl: 'nextParameter.html',
      controller: NextParameterModalCtrl,
      resolve: {
        scope: function () {
          return $scope;
        },
        para:function(){
          return parameter;
        }
      }
    });
  };
  $scope.nextParameters =[];

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.clearNewMeasure = function () {
    $scope.newMeasure = null;
    $scope.initUpdateMeasureStatus();
    $scope.nextParameterInit();
  };


  $scope.isNameDuplicated = function (measures, newMeasure) {
    var names = [];
    for(var i = 0;i < measures.length; i++){
      names.push(measures[i].name);
    }
    var index = names.indexOf(newMeasure.name);
    return (index > -1 && index != $scope.updateMeasureStatus.editIndex);
  }


  $scope.nextParameterInit = function(){
    $scope.nextPara = {
      "type":"column",
      "value":"",
      "next_parameter":null
    }

    if($scope.newMeasure){
      $scope.newMeasure.function.parameter.next_parameter = null;
    }
  }


  $scope.addNewGroupByColumn = function () {
    $scope.nextGroupBy = {
      name:null,
      encoding:"dict",
      valueLength:0,
      encodingName:"dict"
    }
    $scope.convertedColumns.push($scope.nextGroupBy);

  };

  $scope.removeColumn = function(arr,index){
    if (index > -1) {
      arr.splice(index, 1);
    }
  };
  $scope.refreshGroupBy=function (list,index,item) {
    var encoding;
    var name = item.name;
    if(item.encoding=="dict" || item.encoding=="date"|| item.encoding=="time"){
      item.valueLength=0;
    }
  };

  $scope.groupby= function (next_parameter){
    if($scope.num<$scope.convertedColumns.length-1){
      next_parameter.value=$scope.convertedColumns[$scope.num].name;
      next_parameter.type="column";
      next_parameter.next_parameter={};
      $scope.num++;
      $scope.groupby(next_parameter.next_parameter);
    }
    else{
      next_parameter.value=$scope.convertedColumns[$scope.num].name;
      next_parameter.type="column";
      next_parameter.next_parameter=null;
      $scope.num=0;
      return false;
    }
  }
  $scope.converted = function (next_parameter) {
    if (next_parameter != null) {
      $scope.groupby.push(next_parameter.value);
      converted(next_parameter.next_parameter)
    }
    else {
      $scope.groupby.push(next_parameter.value);
      return false;
    }
  }

  $scope.addMeasure = function () {
     // Make a copy of model will be editing.
     $scope.newMeasure = CubeDescModel.createMeasure();
     $scope.measureParamValueColumn=$scope.getCommonMetricColumns();
     $scope.openMeasureModal();
     $scope.addEditMeasureEncoding($scope.newMeasure);
  };

  $scope.editMeasure = function (measure,index) {
     $scope.updateMeasureStatus.editIndex = index;
     $scope.updateMeasureStatus.isEdit = true;
     // Make a copy of model will be editing.
     $scope.newMeasure = angular.copy(measure);
     $scope.addEditMeasureEncoding($scope.newMeasure,$scope.updateMeasureStatus.editIndex);
     $scope.openMeasureModal();
  };

  $scope.openMeasureModal = function () {
     var modalInstance = $modal.open({
        templateUrl: 'addEditDimension.html',
        controller: addEditDimensionCtrl,
        backdrop: 'static',
        scope: $scope
     });

     modalInstance.result.then(function () {
        if (!$scope.updateMeasureStatus.isEdit) {
           $scope.doneAddMeasure();
        } else {
           $scope.doneEditMeasure();
        }
     }, function () {
           $scope.cancelMeasure();
     });
  };
  $scope.doneAddMeasure = function () {
     // Push new dimension which bound user input data.
     $scope.cubeMetaFrame.measures.push(angular.copy($scope.newMeasure));
     $scope.resetParams();
  };

  $scope.doneEditMeasure = function () {
     // Copy edited model to destination model.
     angular.copy($scope.newMeasure, $scope.cubeMetaFrame.measures[$scope.updateMeasureStatus. editIndex]);

     $scope.resetParams();
  };

  $scope.cancelMeasure = function () {
     $scope.resetParams();
  };

  $scope.resetParams = function () {
     $scope.updateMeasureStatus.isEdit = false;
     $scope.updateMeasureStatus. editIndex = -1;
     $scope.newMeasure = null;
     $scope.initUpdateMeasureStatus();
     $scope.nextParameterInit();
  };
  $scope.measureParamValueUpdate = function(){
   if($scope.newMeasure.function.expression !== 'EXTENDED_COLUMN' && $scope.newMeasure.showDim==true){
      $scope.measureParamValueColumn=$scope.getAllModelDimMeasureColumns();
   }
   if($scope.newMeasure.function.expression !== 'EXTENDED_COLUMN' && $scope.newMeasure.showDim==false){
      $scope.measureParamValueColumn=$scope.getCommonMetricColumns();
   }
   if($scope.newMeasure.function.expression == 'EXTENDED_COLUMN'){
      $scope.measureParamValueColumn=$scope.getExtendedHostColumn();
   }
  }
  //map right return type for param
  $scope.measureReturnTypeUpdate = function(){

    if($scope.newMeasure.function.expression == 'TOP_N'){
      if($scope.newMeasure.function.parameter.type==""||!$scope.newMeasure.function.parameter.type){
        $scope.newMeasure.function.parameter.type= 'column';
      }
      $scope.newMeasure.function.returntype = "topn(100)";
      $scope.convertedColumns=[];
      return;
    }else if($scope.newMeasure.function.expression == 'EXTENDED_COLUMN'){
      $scope.newMeasure.function.parameter.type= 'column';
      $scope.newMeasure.function.returntype = "extendedcolumn(100)";
      return;
    }else if($scope.newMeasure.function.expression=='PERCENTILE'){
      $scope.newMeasure.function.parameter.type= 'column';
    }else{
      $scope.nextParameterInit();
    }

    if($scope.newMeasure.function.expression == 'COUNT'){
      $scope.newMeasure.function.parameter.type= 'constant';
    }

    if($scope.newMeasure.function.parameter.type=="constant"&&$scope.newMeasure.function.expression!=="COUNT_DISTINCT"){
      switch($scope.newMeasure.function.expression){
        case "SUM":
        case "COUNT":
          $scope.newMeasure.function.returntype = "bigint";
          break;
        default:
          $scope.newMeasure.function.returntype = "";
          break;
      }
    }
    if($scope.newMeasure.function.parameter.type=="column"&&$scope.newMeasure.function.expression!=="COUNT_DISTINCT"){

      var column = $scope.newMeasure.function.parameter.value;
      if(typeof column=="string"&&column){
        var colType = $scope.getColumnTypeByAlias(StringHelper.removeNameSpace(column), VdmUtil.getNameSpace(column)); // $scope.getColumnType defined in cubeEdit.js
      }
      if(colType==""||!colType){
        $scope.newMeasure.function.returntype = "";
        return;
      }
      switch($scope.newMeasure.function.expression){
        case "SUM":
          if(colType==="smallint"||colType==="int"||colType==="bigint"||colType==="integer"){
            $scope.newMeasure.function.returntype= 'bigint';
          }else{
            if(colType.indexOf('decimal')!=-1){
              $scope.newMeasure.function.returntype= colType;
            }else{
              $scope.newMeasure.function.returntype= 'decimal';
            }
          }
          break;
        case "MIN":
        case "MAX":
          $scope.newMeasure.function.returntype = colType;
          break;
        case "RAW":
          $scope.newMeasure.function.returntype = "raw";
          break;
        case "COUNT":
          $scope.newMeasure.function.returntype = "bigint";
          break;
        case "PERCENTILE":
          $scope.newMeasure.function.returntype = "percentile(100)";
          break;
        default:
          $scope.newMeasure.function.returntype = "";
          break;
      }
    }
  }
var addEditDimensionCtrl = function ($scope, $modalInstance,SweetAlert,language) {
  $scope.dataKylin = language.getDataKylin();
  $scope.ok = function () {
     $modalInstance.close();

  };
  $scope.cancel = function () {
     $modalInstance.dismiss('cancel');
  };
  $scope.checkMeasure = function () {
    if ($scope.newMeasure.function.expression === 'TOP_N' ) {
      if($scope.newMeasure.function.parameter.value == ""){
        SweetAlert.swal('',$scope.dataKylin.alert.warning_column_required , 'warning');
        return false;
      }
      if($scope.convertedColumns.length<1){
        SweetAlert.swal('', $scope.dataKylin.alert.tip_column_required, 'warning');
        return false;
      }
      var hasExisted = [];
      for (var column in $scope.convertedColumns){
        if(hasExisted.indexOf($scope.convertedColumns[column].name)==-1){
          hasExisted.push($scope.convertedColumns[column].name);
        }
        else{
          SweetAlert.swal('', $scope.dataKylin.alert.warning_measures_part_one+$scope.convertedColumns[column].name+$scope.dataKylin.alert.warning_measures_part_two, 'warning');
          return false;
        }
        if($scope.convertedColumns[column].encoding == 'int' && ($scope.convertedColumns[column].valueLength < 1 || $scope.convertedColumns[column].valueLength > 8)) {
          SweetAlert.swal('', $scope.dataKylin.alert.check_cube_rowkeys_int, 'warning');
          return false;
        }
      }
      $scope.nextPara.next_parameter={};
      $scope.newMeasure.function.configuration={};
      $scope.groupby($scope.nextPara);
      angular.forEach($scope.convertedColumns,function(item){
        var a='topn.encoding.'+item.name;
        var versionKey='topn.encoding_version.'+item.name;
        var version=$scope.getTypeVersion(item.encoding);
        var encoding="";
        if(needLengthKeyList.indexOf($scope.removeVersion(item.encoding))>-1){
          encoding = $scope.removeVersion(item.encoding)+":"+item.valueLength;
        }else{
          encoding = $scope.removeVersion(item.encoding);
          item.valueLength=0;
        }
        $scope.newMeasure.function.configuration[a]= encoding;
        $scope.newMeasure.function.configuration[versionKey]=version;

      });
    }

    if ($scope.isNameDuplicated($scope.cubeMetaFrame.measures, $scope.newMeasure) == true) {
      SweetAlert.swal('', $scope.dataKylin.alert.duplicate_measures_part_one + $scope.newMeasure.name + $scope.dataKylin.alert.duplicate_measures_part_two, 'warning');
      return false;
    }

    if($scope.nextPara.value!=="" && ($scope.newMeasure.function.expression == 'EXTENDED_COLUMN' || $scope.newMeasure.function.expression == 'TOP_N')){
      $scope.newMeasure.function.parameter.next_parameter = jQuery.extend(true,{},$scope.nextPara);
    }
    return true;
  }
}
});

var NextParameterModalCtrl = function ($scope, scope,para,$modalInstance,cubeConfig, CubeService, MessageService, $location, SweetAlert,ProjectModel, loadingRequest,ModelService,language,kylinCommon) {
  $scope.dataKylin = language.getDataKylin();
  $scope.newmea={
    "measure":scope.newMeasure
  }

  $scope.cubeConfig = cubeConfig;
  $scope.cancel = function () {
    $modalInstance.dismiss('cancel');
  };

  $scope.getCommonMetricColumns = function(measure){
    return scope.getCommonMetricColumns(measure);
  }

  $scope.nextPara = {
    "type":"",
    "value":"",
    "next_parameter":null
  }

  var _index = scope.nextParameters.indexOf(para);
  if(para){
    $scope.nextPara = para;
  }

  $scope.ok = function(){
    if(_index!=-1){
      scope.nextParameters[_index] = $scope.nextPara;
    }
    else{
      scope.nextParameters.push($scope.nextPara);
    }

    scope.updateNextParameter();
    $modalInstance.dismiss('cancel');
  }

}

