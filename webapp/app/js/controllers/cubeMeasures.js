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

KylinApp.controller('CubeMeasuresCtrl', function ($scope, $modal,MetaModel,cubesManager,CubeDescModel,SweetAlert,kylinCommon) {


  $scope.num=0;
  $scope.convertedColumns=[];
  $scope.groupby=[];

  $scope.initUpdateMeasureStatus = function(){
    $scope.updateMeasureStatus = {
      isEdit:false,
      editIndex:-1
    }
  };
  $scope.initUpdateMeasureStatus();

  $scope.addNewMeasure = function (measure,index) {
    if(measure&&index>=0){
      $scope.updateMeasureStatus.isEdit = true;
      $scope.updateMeasureStatus.editIndex = index;
    }
    $scope.nextParameters = [];
    $scope.newMeasure = (!!measure)? jQuery.extend(true, {},measure):CubeDescModel.createMeasure();
    if(!!measure && measure.function.parameter.next_parameter){
      $scope.nextPara.value = measure.function.parameter.next_parameter.value;
    }
    if($scope.newMeasure.function.expression=="TOP_N"){
      $scope.convertedColumns=[];
            if($scope.newMeasure.function.configuration==null){
                var GroupBy = {
                      name:$scope.newMeasure.function.parameter.next_parameter.value,
                      encoding:"dict",
                      valueLength:0,
                      }
                $scope.convertedColumns.push(GroupBy);
              }

      for(var configuration in $scope.newMeasure.function.configuration) {
        var _name=configuration.slice(14);
        var item=$scope.newMeasure.function.configuration[configuration];
        var _isFixedLength = item.substring(0,12) === "fixed_length"?"true":"false";//fixed_length:12
        var _isIntegerLength = item.substring(0,7) === "integer"?"true":"false";
        var _isIntLength = item.substring(0,3) === "int"?"true":"false";
        var _encoding = item;
        var _valueLength = 0 ;
        if(_isFixedLength !=="false"){
          _valueLength = item.substring(13,item.length);
          _encoding = "fixed_length";
        }

        if(_isIntLength!="false"  && _isIntegerLength=="false" ){
          _valueLength = item.substring(4,item.length);
          _encoding = "int";
        }

        if(_isIntegerLength!="false" ){
            _valueLength = item.substring(8,item.length);
            _encoding = "integer";
        }

        $scope.GroupBy = {
          name:_name,
          encoding:_encoding,
          valueLength:_valueLength,
        }
        $scope.convertedColumns.push($scope.GroupBy);
      };
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

  $scope.saveNewMeasure = function () {
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
        var encoding="";
        if(item.encoding!=="dict" && item.encoding!=="date"&& item.encoding!=="time"){
          if(item.encoding=="fixed_length" && item.valueLength){
            encoding = "fixed_length:"+item.valueLength;
          } else if(item.encoding=="integer" && item.valueLength){
            encoding = "integer:"+item.valueLength;
          }else if(item.encoding=="int" && item.valueLength){
            encoding = "int:"+item.valueLength;
          }else{
            encoding = item.encoding;
          }
        }else{
          encoding = item.encoding;
          item.valueLength=0;
        }
        $scope.newMeasure.function.configuration[a]=encoding;
      });
    }

    if ($scope.isNameDuplicated($scope.cubeMetaFrame.measures, $scope.newMeasure) == true) {
      SweetAlert.swal('', $scope.dataKylin.alert.duplicate_measures_part_one + $scope.newMeasure.name + $scope.dataKylin.alert.duplicate_measures_part_two, 'warning');
      return false;
    }

    if($scope.nextPara.value!=="" && ($scope.newMeasure.function.expression == 'EXTENDED_COLUMN' || $scope.newMeasure.function.expression == 'TOP_N')){
      $scope.newMeasure.function.parameter.next_parameter = jQuery.extend(true,{},$scope.nextPara);
    }

    if($scope.updateMeasureStatus.isEdit == true){
      $scope.cubeMetaFrame.measures[$scope.updateMeasureStatus.editIndex] = $scope.newMeasure;
    }
    else {
      $scope.cubeMetaFrame.measures.push($scope.newMeasure);
    }
    $scope.newMeasure = null;
    $scope.initUpdateMeasureStatus();
    $scope.nextParameterInit();
    return true;
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
      var colType = $scope.getColumnType(column, $scope.metaModel.model.fact_table); // $scope.getColumnType defined in cubeEdit.js

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
        default:
          $scope.newMeasure.function.returntype = "";
          break;
      }
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
