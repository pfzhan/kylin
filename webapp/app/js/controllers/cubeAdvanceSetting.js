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

KylinApp.controller('CubeAdvanceSettingCtrl', function ($scope, $modal,cubeConfig,MetaModel,cubesManager,CubeDescModel,SweetAlert, TableModel,TableExtService,VdmUtil,CubeService) {
  $scope.cubesManager = cubesManager;
  $scope.TableModel=TableModel;
  $scope.cuboidList=[];
  $scope.calcCuboidNumber=function(aggregation_group,index){
    //var needCountNumber=[];
    //var len=metadata.length;
    //for(var i=0;i<len;i++){
    //  var curObj=metadata[i].select_rule;
    //  var lenOfHierarchy=curObj.hierarchy_dims&&curObj.hierarchy_dims.length||0;
    //  var lenOfJoint=curObj.joint_dims&&curObj.joint_dims.length||0;
    //  //计算hierarchy 组合情况
    //  for(var m=0;m<lenOfHierarchy;m++){
    //    var count= 0,childHierachyLen=curObj.hierarchy_dims[m].length||0;
    //    count=childHierachyLen+1;
    //    needCountNumber.push(count);
    //  }
    //  //计算Joint 组合情况
    //  for(var n=0;n<lenOfJoint;n++){
    //    if(curObj.joint_dims[n]&&curObj.joint_dims[n].length){
    //      needCountNumber.push(2);
    //    }
    //  }
    //}
    //var lenOfCountNumber=needCountNumber.length,result=1;
    //while(--lenOfCountNumber>=0){
    //  result=result*needCountNumber[lenOfCountNumber];
    //}
    //var noGroupColumnLen=$scope.cubeMetaFrame.rowkey.rowkey_columns.length-$scope.seletedColumns.length;
    //while(--noGroupColumnLen>=0){
    //  result=result*2;
    //}
    //return result;
    CubeService.calcCuboid({propValue:'aggregationgroups',action:'cuboid'},angular.toJson(aggregation_group,true),function(data){
      $scope.cuboidList[index]=VdmUtil.linkArrObjectToString(data);
    })
  }
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
  var needLengthKeyList=['fixed_length','fixed_length_hex','int','integer'];

  //rowkey
  $scope.convertedRowkeys = [];
  angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(item){
    item.encoding=$scope.removeVersion(item.encoding);
    var _encoding = item.encoding;
    var _valueLength ;
    var tableName=VdmUtil.getNameSpaceTopName(item.column);
    var databaseName=VdmUtil.getNameSpaceTopName($scope.metaModel.model.fact_table);
    var baseKey=item.encoding.replace(/:\d+/,'');
    if(needLengthKeyList.indexOf(baseKey)>-1){
      var result=/:(\d+)/.exec(item.encoding);
      _valueLength=result?result[1]:0;
    }
    _encoding=baseKey;
    angular.forEach($scope.cubeMetaFrame.dimensions,function(dimension){
      if(dimension.derived==null){
        if(dimension.column==VdmUtil.removeNameSpace(item.column)){
          databaseName=VdmUtil.getNameSpaceTopName(dimension.table);
        }
      }else{
        angular.forEach(dimension.derived,function(derived){
          if(derived==VdmUtil.removeNameSpace(item.column)){
            databaseName=VdmUtil.getNameSpaceTopName(dimension.table);
          }
        });
      }
    });
    var rowkeyObj = {
      column:item.column,
      encoding:_encoding+(item.encoding_version?"[v"+item.encoding_version+"]":"[v1]"),
      encodingName:_encoding,
      valueLength:_valueLength,
      isShardBy:item.isShardBy,
      encoding_version:item.encoding_version||1,
      table:tableName,
      database:databaseName
    }
    $scope.convertedRowkeys.push(rowkeyObj);

  })
  setTimeout(function(){
    if($.prototype.colResizable){
      $("#resizeRowkeys").colResizable({
        liveDrag:true,
        gripInnerHtml:"<div class='grip'></div>",
        draggingClass:"dragging",
        resizeMode:'flex',
        partialRefresh:true
      });
    }
  },100)
  $scope.rule={
    shardColumnAvailable:true
  }

  $scope.refreshRowKey = function(list,index,item,checkShard){
    var encoding;
    var column = item.column;
    var isShardBy = item.isShardBy;
    var tableName=VdmUtil.getNameSpaceTopName(item.column);
    var databaseName=VdmUtil.getNameSpaceTopName($scope.metaModel.model.fact_table);
    var version=$scope.getTypeVersion(item.encoding);
    var encodingType=$scope.removeVersion(item.encoding);
    angular.forEach($scope.cubeMetaFrame.dimensions,function(dimension){
      if(dimension.derived==null){
        if(dimension.column==VdmUtil.removeNameSpace(item.column)){
          databaseName=VdmUtil.getNameSpaceTopName(dimension.table);
        }
      }else{
        angular.forEach(dimension.derived,function(derived){
          if(derived==VdmUtil.removeNameSpace(item.column)){
            databaseName=VdmUtil.getNameSpaceTopName(dimension.table);
          }
        });
      }
    });
    if(needLengthKeyList.indexOf(encodingType)>-1){
      encoding = encodingType+":"+item.valueLength;
    }else{
      encoding = encodingType;
      item.valueLength=0;
    }
    $scope.convertedRowkeys[[index]].table=tableName;
    $scope.convertedRowkeys[[index]].database=databaseName;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].column = column;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].encoding = encoding;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].encoding_version =version;
    $scope.cubeMetaFrame.rowkey.rowkey_columns[index].isShardBy = isShardBy;
    if(checkShard == true){
      $scope.checkShardByColumn();
    }

  }

  $scope.checkShardByColumn = function(){
    var shardRowkeyList = [];
    angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(rowkey){
      if(rowkey.isShardBy == true){
        shardRowkeyList.push(rowkey.column);
      }
    })
    if(shardRowkeyList.length >1){
      $scope.rule.shardColumnAvailable = false;
    }else{
      $scope.rule.shardColumnAvailable = true;
    }
  }

  $scope.resortRowkey = function(){
    for(var i=0;i<$scope.convertedRowkeys.length;i++){
      $scope.refreshRowKey($scope.convertedRowkeys,i,$scope.convertedRowkeys[i]);
    }
  }

  $scope.removeRowkey = function(arr,index,item){
    if (index > -1) {
      arr.splice(index, 1);
    }
    $scope.cubeMetaFrame.rowkey.rowkey_columns.splice(index,1);
  }


  $scope.addNewRowkeyColumn = function () {
    var rowkeyObj = {
      column:"",
      encoding:"dict",
      valueLength:0,
      isShardBy:"false",
      table:"",
      database:VdmUtil.getNameSpaceTopName($scope.metaModel.model.fact_table)
    }

    $scope.convertedRowkeys.push(rowkeyObj);
    $scope.cubeMetaFrame.rowkey.rowkey_columns.push({
      column:'',
      encoding:'dict',
      isShardBy:'false',
      index: 'eq'
    });

  };
  $scope.addNewHierarchy = function(grp){
    grp.select_rule.hierarchy_dims.push([]);
  }

  $scope.addNewJoint = function(grp){
    grp.select_rule.joint_dims.push([]);
  }
  $scope.seletedColumns=[];
  $scope.seletedInclues=[];
  $scope.cuboidCount='';
  //to do, agg update
  $scope.addNewAggregationGroup = function () {
    $scope.cubeMetaFrame.aggregation_groups.push(CubeDescModel.createAggGroup());
    $scope.seletedColumns=$scope.calcSelectedColums($scope.cubeMetaFrame.aggregation_groups);
    $scope.seletedInclues=$scope.calcSelectedIncludes($scope.cubeMetaFrame.aggregation_groups);
    //$scope.cuboidCount=$scope.calcCuboidNumber($scope.cubeMetaFrame.aggregation_groups);
  };

  $scope.refreshAggregationGroup = function (list, index, aggregation_groups) {
    if (aggregation_groups) {
      list[index] = aggregation_groups;
    }
    $scope.seletedColumns=$scope.calcSelectedColums(list);
    $scope.seletedInclues=$scope.calcSelectedIncludes(list);
    //$scope.cuboidCount=$scope.calcCuboidNumber(list);
    $scope.calcCuboidNumber(aggregation_groups,index);
  };

  $scope.refreshAggregationHierarchy = function (list, index, aggregation_group,hieIndex,hierarchy) {
    if(hierarchy){
      aggregation_group.select_rule.hierarchy_dims[hieIndex] = hierarchy;
    }
    if (aggregation_group) {
      list[index] = aggregation_group;
    }
    $scope.seletedColumns=$scope.calcSelectedColums(list);
    $scope.seletedInclues=$scope.calcSelectedIncludes(list);
    $scope.calcCuboidNumber(aggregation_group,index)
  };

  $scope.refreshAggregationJoint = function (list, index, aggregation_group,joinIndex,jointDim){
    if(jointDim){
      aggregation_group.select_rule.joint_dims[joinIndex] = jointDim;
    }
    if (aggregation_group) {
      list[index] = aggregation_group;
    }
    $scope.seletedColumns=$scope.calcSelectedColums(list);
    $scope.seletedInclues=$scope.calcSelectedIncludes(list);
    $scope.calcCuboidNumber(aggregation_group,index);
  };

  $scope.refreshIncludes = function (list, index, aggregation_groups) {
    if (aggregation_groups) {
      list[index] = aggregation_groups;
    }
  };

  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.removeHierarchy = function(arr,element){
    var index = arr.select_rule.hierarchy_dims.indexOf(element);
    if(index>-1){
      arr.select_rule.hierarchy_dims.splice(index,1);
    }

  }

  $scope.removeJointDims = function(arr,element){
    var index = arr.select_rule.joint_dims.indexOf(element);
    if(index>-1){
      arr.select_rule.joint_dims.splice(index,1);
    }

  }



  $scope.isReuse=false;
  $scope.addNew=false;


  $scope.newDictionaries = {
    "column":null,
    "builder": null,
    "reuse": null
  }



  $scope.initUpdateDictionariesStatus = function(){
    $scope.updateDictionariesStatus = {
      isEdit:false,
      editIndex:-1
    }
  };
  $scope.initUpdateDictionariesStatus();


  $scope.addNewDictionaries = function (dictionaries, index) {
    if(dictionaries&&index>=0){
      $scope.updateDictionariesStatus.isEdit = true;
      $scope.addNew=true;
      $scope.updateDictionariesStatus.editIndex = index;
      if(dictionaries.builder==null){
        $scope.isReuse=true;
      }
      else{
        $scope.isReuse=false;
      }
    }
    else{
      $scope.addNew=!$scope.addNew;
    }
    $scope.newDictionaries = (!!dictionaries)? jQuery.extend(true, {},dictionaries):CubeDescModel.createDictionaries();
  };

  $scope.saveNewDictionaries = function (){
    if(!$scope.cubeMetaFrame.dictionaries){
      $scope.cubeMetaFrame.dictionaries=[];
    }

    if($scope.updateDictionariesStatus.isEdit == true) {
      if ($scope.cubeMetaFrame.dictionaries[$scope.updateDictionariesStatus.editIndex].column != $scope.newDictionaries.column) {
        if(!$scope.checkColumn()){
          return false;
        }
      }
      else {
        $scope.cubeMetaFrame.dictionaries[$scope.updateDictionariesStatus.editIndex] = $scope.newDictionaries;
      }
    }
    else
    {
      if(!$scope.checkColumn()){
        return false;
      }
      $scope.cubeMetaFrame.dictionaries.push($scope.newDictionaries);
    }
    $scope.newDictionaries = null;
    $scope.initUpdateDictionariesStatus();
    $scope.nextDictionariesInit();
    $scope.addNew = !$scope.addNew;
    $scope.isReuse = false;
    return true;

  };

  $scope.nextDictionariesInit = function(){
    $scope.nextDic = {
      "coiumn":null,
      "builder":null,
      "reuse":null
    }
  }
  $scope.checkColumn = function (){
    var isColumnExit=false;
    angular.forEach($scope.cubeMetaFrame.dictionaries,function(dictionaries){
      if(!isColumnExit){
        if(dictionaries.column==$scope.newDictionaries.column)
          isColumnExit=true;
      }
    })
    if(isColumnExit){
      SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.warning_dictionaries_part_one + $scope.newDictionaries.column + $scope.dataKylin.alert.warning_dictionaries_part_two, 'warning');
      return false;
    }
    return true;
  }
  $scope.clearNewDictionaries = function (){
    $scope.newDictionaries = null;
    $scope.isReuse=false;
    $scope.initUpdateDictionariesStatus();
    $scope.nextDictionariesInit();
    $scope.addNew=!$scope.addNew;
  }

  $scope.change = function (){
    $scope.newDictionaries.builder=null;
    $scope.newDictionaries.reuse=null;
    $scope.isReuse=!$scope.isReuse;
  }

  $scope.removeDictionaries =  function(arr,element){
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };
  $scope.calcSelectedColums=function(metadata){
    var arr=[];
    var len=metadata.length;
    for(var i=0;i<len;i++){
      var curObj=metadata[i].select_rule;
      arr=arr.concat(curObj.mandatory_dims||[]);
      var lenOfHierarchy=curObj.hierarchy_dims&&curObj.hierarchy_dims.length||0;
      var lenOfJoint=curObj.joint_dims&&curObj.joint_dims.length||0;
      for(var m=0;m<lenOfHierarchy;m++){
        arr=arr.concat(curObj.hierarchy_dims[m]||[]);
      }
      for(var n=0;n<lenOfJoint;n++){
        arr=arr.concat(curObj.joint_dims[n]||[]);
      }
    }
    return arr;
  }
  $scope.calcSelectedIncludes=function(metadata){
    var arr=[];
    var len=metadata.length;
    for(var i=0;i<len;i++){
      var curObj=metadata[i].includes;
      arr=arr.concat(curObj);
    }
    return arr;
  }

  $scope.seletedColumns=$scope.calcSelectedColums($scope.cubeMetaFrame.aggregation_groups);
  //$scope.cuboidCount=$scope.calcCuboidNumber($scope.cubeMetaFrame.aggregation_groups);


  var container=$("#advancedSettingInfoBox table").eq(1).parent();
  //show column info
  $scope.showColumnInfo=function(selectIndex,dataArr){
     var columnName=dataArr[selectIndex];
     var tableName=$scope.getFullTaleNameByColumnName(columnName);
     $scope.getSampleDataByTableName(tableName,function(){
       $scope.tableName=tableName;
       $scope.columnName=columnName;
       $scope.columnType= TableModel.columnNameTypeMap[VdmUtil.removeNameSpace(columnName)]||''
       $scope.columnList=TableModel.getColumnsByTable(VdmUtil.removeNameSpace(tableName));
        $scope.columnList.map(function(obj,k){
         if(VdmUtil.removeNameSpace(tableName)+'.'+obj.name==columnName){
           $scope.columnIndex=k;
           return false;
         }
       });
       var thColumn=$("#"+VdmUtil.removeNameSpace(columnName));
       if(thColumn&&thColumn.length){
         autoScroll(container,thColumn);
       }else{
         setTimeout(function(){
           container=$("#advancedSettingInfoBox table").eq(1).parent();
           autoScroll(container,$("#"+VdmUtil.removeNameSpace(columnName)));
         },10)
       }
     });
  }
  $scope.getSampleDataByTableName=function(tableName,callback){
    TableExtService.getSampleInfo({tableName:tableName},function(data){
      var sampleData=[],specialData=[];
      if(data.sample_rows&&data.sample_rows.length){
        sampleData=sampleData.concat(VdmUtil.changeDataAxis(data.sample_rows,true));
      }
      $scope.specialData=data.columns_stats;
      $scope.sampleData=sampleData;
      if(typeof callback=='function'){
        callback();
      }
    })
  }
  $scope.getFullTaleNameByColumnName=function(columnName){
    for(var i= 0,len=$scope.cubeMetaFrame.dimensions.length||0;i<len;i++){
      if(($scope.cubeMetaFrame.dimensions[i].table+'.'+$scope.cubeMetaFrame.dimensions[i].column).indexOf(columnName)>=0){
         return $scope.cubeMetaFrame.dimensions[i].table;
      }
    }
    return "";
  }

  function autoScroll(container,scrollTo){
    if(scrollTo&&scrollTo.length){
      container.animate({
        scrollLeft: scrollTo.offset().left - container.offset().left + container.scrollLeft()
      });
      $("#advancedSettingInfoBox table th").css('color','#000');
      scrollTo.css('color','#3276b1');
    }
  }
  //初始化已有分组的cuboid
  for(var i=0;i<$scope.cubeMetaFrame.aggregation_groups.length;i++){
    $scope.calcCuboidNumber($scope.cubeMetaFrame.aggregation_groups[i],i);
  }

});
