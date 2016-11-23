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

KylinApp.controller('CubeOverWriteCtrl', function ($scope, $modal,cubeConfig,MetaModel,cubesManager,CubeDescModel,CubeConfigService) {
  $scope.cubesManager = cubesManager;

  $scope.convertedProperties=[];
  //rowkey
  if(angular.equals({},$scope.cubeMetaFrame.override_kylin_properties)&&$scope.state.mode!="view"){
    $scope.convertedProperties=[
      {name:"kylin.storage.hbase.compression-codec",value:""},
      {name:"kylin.job.sampling-percentage",value:""},
      {name:"kylin.cube.algorithm",value:""},
      {name:"kylin.cube.aggrgroup.max-combination",value:""}
    ]
    for(var i=0;i<$scope.convertedProperties.length;i++){
      (function(i){
        CubeConfigService.getDefault({key:$scope.convertedProperties[i].name},function(data){
          var str="";
          for(var s in data){
            if(s&&+s==+s){
              str+=data[s]
            }
          }
          $scope.convertedProperties[i].value=str;
          $scope.cubeMetaFrame.override_kylin_properties[$scope.convertedProperties[i].name]=str;
        })
      }(i))
    }
  }

  for(var key in $scope.cubeMetaFrame.override_kylin_properties){
    $scope.convertedProperties.push({
      name:key,
      value:$scope.cubeMetaFrame.override_kylin_properties[key]
    });
  }


  $scope.addNewProperty = function () {
    if($scope.cubeMetaFrame.override_kylin_properties.hasOwnProperty('')){
      return;
    }
    $scope.cubeMetaFrame.override_kylin_properties['']='';
    $scope.convertedProperties.push({
      name:'',
      value:''
    });

  };

  $scope.refreshPropertiesObj = function(){
    $scope.cubeMetaFrame.override_kylin_properties = {};
    angular.forEach($scope.convertedProperties,function(item,index){
      $scope.cubeMetaFrame.override_kylin_properties[item.name] = item.value;
    })
  }


  $scope.refreshProperty = function(list,index,item){
    $scope.convertedProperties[index] = item;
    $scope.refreshPropertiesObj();
  }


  $scope.removeProperty= function(arr,index,item){
    if (index > -1) {
      arr.splice(index, 1);
    }
    delete $scope.cubeMetaFrame.override_kylin_properties[item.name];
  }



});
