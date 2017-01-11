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

KylinApp.controller('CubeCtrl', function ($scope, AccessService, MessageService, CubeService, TableService, ModelGraphService, UserService,SweetAlert,loadingRequest,modelsManager,$modal,cubesManager,kylinCommon) {
    $scope.newAccess = null;
    $scope.state = {jsonEdit: false};

    $scope.modelsManager = modelsManager;
    $scope.cubesManager = cubesManager;

    $scope.buildGraph = function (cube) {
       ModelGraphService.buildTree(cube);
    };

    $scope.getCubeSql = function (cube) {
        CubeService.getSql({cubeId: cube.name, propValue: "null"}, function (sql) {
            cube.sql = sql.sql;
        },function(e){
          kylinCommon.error_default(e);
        });
    };

    $scope.getNotifyListString = function (cube) {
        if (cube.detail.notify_list) {
            cube.notifyListString = cube.detail.notify_list.join(",");
        }
        else {
            cube.notifyListString = "";
        }
    };

    $scope.cleanStatus = function(cube){

        if (!cube)
        {
            return;
        }
        var newCube = jQuery.extend(true, {}, cube);
        delete newCube.project;

        angular.forEach(newCube.dimensions, function(dimension, index){
            delete dimension.status;
        });

        return newCube;
    };

    $scope.updateNotifyList = function (cube) {
        cube.detail.notify_list = cube.notifyListString.split(",");
        CubeService.updateNotifyList({cubeId: cube.name}, cube.detail.notify_list, function () {
          kylinCommon.success_alert($scope.dataKylin.alert.success,$scope.dataKylin.alert.success_notify_list_updated);
        },function(e){
          kylinCommon.error_default(e);
        });
    };

    $scope.getHbaseInfo = function (cube) {

        cube.tablemap = {};
        angular.forEach(cube.segments,function(seg){
            cube.tablemap[seg.storage_location_identifier] =  seg.uuid;
        })

        if (!cube.hbase) {
          if(+cube.detail.storage_type==100&&+cube.detail.engine_type==100||+cube.detail.storage_type==99&&+cube.detail.engine_type==99){
            CubeService.getColumnarInfo({cubeId: cube.name, propValue: null, action: null}, function (hbase) {
              cube.hbase = hbase;
              cube.type="columnar";
              // Calculate cube total size based on each htable.
              var totalSize = 0;
              hbase.forEach(function(t) {
                totalSize += t.storageSize;
                if(t.rawTableStorageSize){
                  totalSize += t.rawTableStorageSize;
                }
              });
              cube.totalSize = totalSize;
            },function(e){
              kylinCommon.error_default(e);
            });
          }else{
            CubeService.getHbaseInfo({cubeId: cube.name, propValue: null, action: null}, function (hbase) {
              cube.hbase = hbase;
              cube.type="hbase";
              // Calculate cube total size based on each htable.
              var totalSize = 0;
              hbase.forEach(function(t) {
                totalSize += t.tableSize;
                if(t.rawTableStorageSize){
                  totalSize += t.rawTableStorageSize;
                }
              });
              cube.totalSize = totalSize;
            },function(e){
              kylinCommon.error_default(e);
            });
          }
        }
    };

});

