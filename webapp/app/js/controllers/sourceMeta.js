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

KylinApp
  .controller('SourceMetaCtrl', function ($scope, $cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService, $route, loadingRequest, SweetAlert, tableConfig, TableModel,cubeConfig,kylinCommon,TableExtService,VdmUtil) {
    var $httpDefaultCache = $cacheFactory.get('$http');
    $scope.tableModel = TableModel;
    $scope.tableModel.selectedSrcDb = [];
    $scope.tableModel.selectedSrcTable = {};
    $scope.window = 0.58 * $window.innerHeight;
    $scope.tableConfig = tableConfig;


    $scope.state = {
      filterAttr: 'id', filterReverse: false, reverseColumn: 'id',
      dimensionFilter: '', measureFilter: ''
    };

    function innerSort(a, b) {
      var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
      if (nameA < nameB) //sort string ascending
        return -1;
      if (nameA > nameB)
        return 1;
      return 0; //default return value (no sorting)
    };

    $scope.aceSrcTbLoaded = function (forceLoad) {
      //stop request when project invalid
      if (!$scope.projectModel.getSelectedProject()) {
        TableModel.init();
        return;
      }
      if (forceLoad) {
        $httpDefaultCache.removeAll();
      }
      $scope.loading = true;
      TableModel.aceSrcTbLoaded(forceLoad).then(function () {
        $scope.loading = false;
      });
    };

    $scope.$watch('projectModel.selectedProject', function (newValue, oldValue) {
//         will load table when enter this page,null or not
      $scope.aceSrcTbLoaded();
    }, function (resp) {
      SweetAlert.swal($scope.dataKylin.alert.oops, resp, 'error');
    });


    $scope.showSelected = function (obj) {
      if (obj.uuid) {
        $scope.tableModel.selectedSrcTable = obj;
      }
      else if (obj.datatype) {
        $scope.tableModel.selectedSrcTable.selectedSrcColumn = obj;
      }
    };
    //收集流数据
    $scope.calcStreamingSampleData=function(tableName){
      $scope.$broadcast('reloadSampleData', tableName);
    }
    $scope.aceSrcTbChanged = function () {
      $scope.tableModel.selectedSrcDb = [];
      $scope.tableModel.selectedSrcTable = {};
      $scope.aceSrcTbLoaded(true);
    };
    //$( ".selector" ).sortable({ disabled: false });

    $scope.openModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_project, 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addHiveTable.html',
        controller: ModalInstanceCtrl,
        backdrop : 'static',
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName: function () {
            return $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };

    $scope.openTreeModal = function () {
      if(!$scope.projectModel.selectedProject){
        SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_project, 'info');
        return;
      }
      $modal.open({
        templateUrl: 'addHiveTableFromTree.html',
        controller: ModalInstanceCtrl,
        windowClass:"tableFromTree",
        resolve: {
          tableNames: function () {
            return $scope.tableNames;
          },
          projectName:function(){
            return  $scope.projectModel.selectedProject;
          },
          scope: function () {
            return $scope;
          }
        }
      });
    };
    $scope.reloadTable = function (tableNames,projectName){

      SweetAlert.swal({
        title: "",
        text: $scope.dataKylin.alert.loadTableWidthCollectConfirm,
        showCancelButton: true,
        confirmButtonColor: '#DD6B55',
        confirmButtonText: "Yes",
        cancelButtonText: "No",
        closeOnConfirm: true
      }, function (isConfirm) {
        var delay = $q.defer();
        loadingRequest.show();
        TableExtService.loadHiveTable({tableName: tableNames, action: projectName}, {
          calculate:isConfirm
        }, function (result) {
          var loadTableInfo = "";
          angular.forEach(result['result.loaded'], function (table) {
            loadTableInfo += "\n" + table;
          });
          var unloadedTableInfo = "";
          angular.forEach(result['result.unloaded'], function (table) {
            unloadedTableInfo += "\n" + table;
          });
          if (result['result.unloaded'].length != 0 && result['result.loaded'].length == 0) {
            SweetAlert.swal('Failed!', 'Failed to load following table(s): ' + unloadedTableInfo, 'error');
          }
          if (result['result.loaded'].length != 0 && result['result.unloaded'].length == 0) {
            SweetAlert.swal('Success!', 'The following table(s) have been successfully loaded: ' + loadTableInfo, 'success');
          }
          if (result['result.loaded'].length != 0 && result['result.unloaded'].length != 0) {
            SweetAlert.swal('Partial loaded!', 'The following table(s) have been successfully loaded: ' + loadTableInfo + "\n\n Failed to load following table(s):" + unloadedTableInfo, 'warning');
          }
          loadingRequest.hide();
          delay.resolve("");
        }, function (e) {
          kylinCommon.error_default(e);
          loadingRequest.hide();
        })
        return delay.promise;
      })
    }

    $scope.unloadTable = function (tableNames,projectName) {
      loadingRequest.show();
      TableExtService.unLoadHiveTable({tableName: tableNames, action: projectName}, {}, function (result) {
        var removedTableInfo = tableNames;
        kylinCommon.success_alert($scope.dataKylin.alert.success, $scope.dataKylin.alert.tip_partial_loaded_body_part_one + removedTableInfo);
        loadingRequest.hide();
        $scope.aceSrcTbLoaded(true);
      }, function (e) {
        kylinCommon.error_default(e);
        loadingRequest.hide();
      })
    }


    var ModalInstanceCtrl = function ($scope, $location, $modalInstance, tableNames, MessageService, projectName, scope,kylinConfig,language,kylinCommon) {
      $scope.dataKylin = language.getDataKylin();
      $scope.tableNames = "";
      $scope.projectName = projectName;
      $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
      };

      $scope.kylinConfig = kylinConfig;


      $scope.treeOptions = {multiSelection: true};
      $scope.selectedNodes = [];
      $scope.hiveLimit = kylinConfig.getHiveLimit();

      $scope.loadHive = function () {
        if ($scope.hiveLoaded)
          return;
        TableService.showHiveDatabases({}, function (databases) {
          $scope.dbNum = databases.length;
          if (databases.length > 0) {
            $scope.hiveMap = {};
            for (var i = 0; i < databases.length; i++) {
              var dbName = databases[i];
              var hiveData = {"dbname": dbName, "tables": [], "expanded": false};
              $scope.hive.push(hiveData);
              $scope.hiveMap[dbName] = i;
            }
          }
          $scope.hiveLoaded = true;
          $scope.showMoreDatabases();
        });
      }

      $scope.showMoreTables = function (hiveTables, node) {
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if (from + $scope.hiveLimit > hiveTables.length) {
          to = hiveTables.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if (!angular.isUndefined(node.children[from])) {
          node.children.pop();
        }

        for (var idx = from; idx <= to; idx++) {
          node.children.push({"label": node.label + '.' + hiveTables[idx], "id": idx - from + 1, "children": []});
        }

        if (hasMore) {
          var loading = {"label": "", "id": 65535, "children": []};
          node.children.push(loading);
        }
      }

      $scope.showAllTables = function (hiveTables, node) {
        var shownTimes = parseInt(node.children.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = hiveTables.length - 1;
        if (!angular.isUndefined(node.children[from])) {
          node.children.pop();
        }
        for (var idx = from; idx <= to; idx++) {
          node.children.push({"label": node.label + '.' + hiveTables[idx], "id": idx - from + 1, "children": []});
        }
      }

      $scope.showMoreDatabases = function () {
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = 0;
        var hasMore = false;
        if (from + $scope.hiveLimit > $scope.hive.length) {
          to = $scope.hive.length - 1;
        } else {
          to = from + $scope.hiveLimit - 1;
          hasMore = true;
        }
        if (!angular.isUndefined($scope.treedata[from])) {
          $scope.treedata.pop();
        }

        for (var idx = from; idx <= to; idx++) {
          var children = [];
          var loading = {"label": "", "id": 0, "children": []};
          children.push(loading);
          $scope.treedata.push({
            "label": $scope.hive[idx].dbname,
            "id": idx + 1,
            "children": children,
            "expanded": false
          });
        }

        if (hasMore) {
          var loading = {"label": "", "id": 65535, "children": [0]};
          $scope.treedata.push(loading);
        }
      }

      $scope.showAllDatabases = function () {
        var shownTimes = parseInt($scope.treedata.length / $scope.hiveLimit);
        var from = $scope.hiveLimit * shownTimes;
        var to = $scope.hive.length - 1;

        if (!angular.isUndefined($scope.treedata[from])) {
          $scope.treedata.pop();
        }

        for (var idx = from; idx <= to; idx++) {
          var children = [];
          var loading = {"label": "", "id": 0, "children": []};
          children.push(loading);
          $scope.treedata.push({
            "label": $scope.hive[idx].dbname,
            "id": idx + 1,
            "children": children,
            "expanded": false
          });
        }
      }

      $scope.showMoreClicked = function ($parentNode) {
        if ($parentNode == null) {
          $scope.showMoreDatabases();
        } else {
          $scope.showMoreTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables, $parentNode);
        }
      }

      $scope.showAllClicked = function ($parentNode) {
        if ($parentNode == null) {
          $scope.showAllDatabases();
        } else {
          $scope.showAllTables($scope.hive[$scope.hiveMap[$parentNode.label]].tables, $parentNode);
        }
      }

      $scope.showToggle = function (node) {
        if (node.expanded == false) {
          TableService.showHiveTables({"database": node.label}, function (hive_tables) {
            var tables = [];
            for (var i = 0; i < hive_tables.length; i++) {
              tables.push(hive_tables[i]);
            }
            $scope.hive[$scope.hiveMap[node.label]].tables = tables;
            $scope.showMoreTables(tables, node);
            node.expanded = true;
          });
        }
      }

      $scope.showSelected = function (node) {

      }

      if (angular.isUndefined($scope.hive) || angular.isUndefined($scope.hiveLoaded) || angular.isUndefined($scope.treedata)) {
        $scope.hive = [];
        $scope.hiveLoaded = false;
        $scope.treedata = [];
        $scope.loadHive();
      }


      $scope.add = function () {

        if ($scope.tableNames.length === 0 && $scope.selectedNodes.length > 0) {
          for (var i = 0; i < $scope.selectedNodes.length; i++) {
            if ($scope.selectedNodes[i].label.indexOf(".") >= 0) {
              $scope.tableNames += ($scope.selectedNodes[i].label) += ',';
            }
          }
        }

        if ($scope.tableNames.trim() === "") {
          SweetAlert.swal('', $scope.dataKylin.alert.tip_to_synchronize, 'info');
          return;
        }

        if (!$scope.projectName) {
          SweetAlert.swal('', $scope.dataKylin.alert.tip_choose_project, 'info');
          return;
        }

        $scope.cancel();
        scope.reloadTable($scope.tableNames,projectName).then(function(){
          scope.aceSrcTbLoaded(true);
        });
      }

    };


      //streaming model
      $scope.openStreamingSourceModal = function () {
        if (!$scope.projectModel.selectedProject) {
          SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.tip_select_project, 'info');
          return;
        }
        $modal.open({
          templateUrl: 'addStreamingSource.html',
          controller: StreamingSourceCtrl,
          backdrop: 'static',
          resolve: {
            tableNames: function () {
              return $scope.tableNames;
            },
            projectName: function () {
              return $scope.projectModel.selectedProject;
            },
            scope: function () {
              return $scope;
            }
          }
        });
      };

      $scope.editStreamingConfig = function (tableName) {
        var modalInstance = $modal.open({
          templateUrl: 'editStreamingSource.html',
          controller: EditStreamingSourceCtrl,
          backdrop: 'static',
          resolve: {
            tableNames: function () {
              return $scope.tableNames;
            },
            projectName: function () {
              return $scope.projectModel.selectedProject;
            },
            tableName: function () {
              return tableName;
            },
            scope: function () {
              return $scope;
            }
          }
        });

        modalInstance.result.then(function () {
          $scope.$broadcast('StreamingConfigEdited');
        }, function () {
          $scope.$broadcast('StreamingConfigEdited');
        });


      }


      var EditStreamingSourceCtrl = function ($scope, language, $interpolate, $templateCache, tableName, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig, cubeConfig, StreamingModel, StreamingService,ClusterService) {


        $scope.dataKylin = language.getDataKylin();
        $scope.state = {
          tableName: tableName,
          mode: "edit",
          target: "kfkConfig"
        }
        $scope.rule = {
          'timestampColumnExist': false
        }
        $scope.streaming = {
          sourceSchema: '',
          'parseResult': {}
        }
        $scope.streamingCfg = {
          parseTsColumn: "{{}}",
          columnOptions: []
        }
        $scope.table = {
          name: '',
          sourceValid: false,
          schemaChecked: false,
          database:'DEFAULT',
          message:[],
          columnList:[]
        }
        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

        $scope.projectName = projectName;
        $scope.streamingMeta = StreamingModel.createStreamingConfig();
        $scope.kafkaMeta = StreamingModel.createKafkaConfig();
        $scope.updateStreamingMeta = function (val) {
          $scope.streamingMeta = val;
        }
        $scope.updateKafkaMeta = function (val) {
          $scope.kafkaMeta = val;
        }

        $scope.streamingResultTmpl = function (notification) {
          // Get the static notification template.
          var tmpl = notification.type == 'success' ? 'streamingResultSuccess.html' : 'streamingResultError.html';
          return $interpolate($templateCache.get(tmpl))(notification);
        };
        $scope.setSampleData=function(table,callback,errcallback){
          ClusterService.saveSampleData({
            table:table,
            action:'samples'
          },$scope.table.message,function(data){
            if(data&&data[0]+data[1]=='ok'){
              callback();
            }else{
              errcallback();
            }
          },function(){
            errcallback();
          })
        }
        $scope.updateStreamingSchema = function () {
          StreamingService.update({}, {
            project: $scope.projectName,
            tableData: angular.toJson(""),
            streamingConfig: angular.toJson($scope.streamingMeta),
            kafkaConfig: angular.toJson($scope.kafkaMeta)
          }, function (request) {
            if (request.successful) {
              $scope.setSampleData($scope.table.database+'.'+$scope.table.name,function(){
                SweetAlert.swal('', 'Updated the streaming successfully.', 'success');
              },function(){
                SweetAlert.swal('', 'Updated the streaming successfully.', 'success');
              });
              $scope.cancel();
            } else {
              var message = request.message;
              var msg = !!(message) ? message : 'Failed to take action.';
              MessageService.sendMsg($scope.streamingResultTmpl({
                'text': msg,
                'streamingSchema': angular.toJson($scope.streamingMeta, true),
                'kfkSchema': angular.toJson($scope.kafkaMeta, true)
              }), 'error', {}, true, 'top_center');
            }
            loadingRequest.hide();
          }, function (e) {
            if (e.data && e.data.exception) {
              var message = e.data.exception;
              var msg = !!(message) ? message : 'Failed to take action.';
              MessageService.sendMsg($scope.streamingResultTmpl({
                'text': msg,
                'streamingSchema': angular.toJson($scope.streamingMeta, true),
                'kfkSchema': angular.toJson($scope.kafkaMeta, true)
              }), 'error', {}, true, 'top_center');
            } else {
              MessageService.sendMsg($scope.streamingResultTmpl({
                'text': msg,
                'streamingSchema': angular.toJson($scope.streamingMeta, true),
                'kfkSchema': angular.toJson($scope.kafkaMeta, true)
              }), 'error', {}, true, 'top_center');
            }
            //end loading
            loadingRequest.hide();

          })
        }




      }


      var StreamingSourceCtrl = function ($scope, $location, $interpolate, $templateCache, $modalInstance, tableNames, MessageService, projectName, scope, tableConfig, cubeConfig, StreamingModel, StreamingService, language, kylinCommon,ClusterService) {
        $scope.dataKylin = language.getDataKylin();
        $scope.state = {
          'mode': 'edit'
        }

        $scope.streamingMeta = StreamingModel.createStreamingConfig();
        $scope.kafkaMeta = StreamingModel.createKafkaConfig();

        $scope.streamingCfg = {
          parseTsColumn: "{{}}",
          columnOptions: []
        }
        $scope.columnList=[];

        $scope.projectName = projectName;
        $scope.streaming = {
          sourceSchema: '',
          'parseResult': {}
        }

        $scope.table = {
          name: '',
          sourceValid: false,
          schemaChecked: false,
          database:'DEFAULT',
          message:[],
          columnList:[]
        }

        $scope.cancel = function () {
          $modalInstance.dismiss('cancel');
        };

        $scope.streamingOnLoad = function () {

        }




        $scope.streamingResultTmpl = function (notification) {
          // Get the static notification template.
          var tmpl = notification.type == 'success' ? 'streamingResultSuccess.html' : 'streamingResultError.html';
          return $interpolate($templateCache.get(tmpl))(notification);
        };


        $scope.form = {};
        $scope.rule = {
          'timestampColumnExist': false
        }

        $scope.modelMode == "addStreaming";

        $scope.syncStreamingSchema = function () {

          $scope.form['cube_streaming_form'].$submitted = true;

          if ($scope.form['cube_streaming_form'].parserName.$invalid || $scope.form['cube_streaming_form'].parserProperties.$invalid) {
            $scope.state.isParserHeaderOpen = true;
          }

          if ($scope.form['cube_streaming_form'].$invalid||!$scope.rule.timestampColumnExist) {
            return;
          }

          var columns = [];
          angular.forEach($scope.table.columnList, function (column, $index) {
            if (column.checked == "Y") {
              var columnInstance = {
                "id": ++$index,
                "name": column.name,
                "datatype": column.type
              }
              columns.push(columnInstance);
            }
          })


          $scope.tableData = {
            "name": $scope.table.name,
            "source_type": 1,
            "columns": columns,
            'database': $scope.table.database||'Default'
          }


          $scope.kafkaMeta.name = $scope.table.name
          $scope.streamingMeta.name = $scope.table.name;

          SweetAlert.swal({
            title: "",
            text: $scope.dataKylin.alert.tip_save_streaming_table,
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: true
          }, function (isConfirm) {
            if (isConfirm) {
              loadingRequest.show();
              $scope.setSampleData=function(table,callback,errcallback){
                ClusterService.saveSampleData({
                  table:table,
                  action:'samples'
                },$scope.table.message,function(data){
                  if(data&&data[0]+data[1]=='ok'){
                    callback();
                  }else{
                    errcallback();
                  }
                },function(){
                  errcallback();
                })
              }
                //$scope.kafkaMeta.messages=angular.fromJson('['+$scope.table.message.join(',')+']');
                StreamingService.save({}, {
                  project: $scope.projectName,
                  tableData: angular.toJson($scope.tableData),
                  streamingConfig: angular.toJson($scope.streamingMeta),
                  kafkaConfig: angular.toJson($scope.kafkaMeta)
                }, function (request) {
                  if (request.successful) {
                    $scope.setSampleData($scope.table.database+'.'+$scope.table.name,function(){
                      SweetAlert.swal('', $scope.dataKylin.alert.tip_created_streaming, 'success');
                      scope.aceSrcTbLoaded(true);
                    },function(){
                      SweetAlert.swal('', $scope.dataKylin.alert.tip_created_streaming, 'success');
                      scope.aceSrcTbLoaded(true);
                    });
                    $scope.cancel();

                  } else {
                    var message = request.message;
                    var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;
                    MessageService.sendMsg($scope.streamingResultTmpl({
                      'text': msg,
                      'streamingSchema': angular.toJson($scope.streamingMeta, true),
                      'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                    }), 'error', {}, true, 'top_center');
                  }
                  loadingRequest.hide();
                }, function (e) {
                  if (e.data && e.data.exception) {
                    var message = e.data.exception;
                    var msg = !!(message) ? message : $scope.dataKylin.alert.error_info;

                    MessageService.sendMsg($scope.streamingResultTmpl({
                      'text': msg,
                      'streamingSchema': angular.toJson($scope.streamingMeta, true),
                      'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                    }), 'error', {}, true, 'top_center');
                  } else {
                    MessageService.sendMsg($scope.streamingResultTmpl({
                      'text': msg,
                      'streamingSchema': angular.toJson($scope.streamingMeta, true),
                      'kfkSchema': angular.toJson($scope.kafkaMeta, true)
                    }), 'error', {}, true, 'top_center');
                  }
                  //end loading
                  loadingRequest.hide();
                })
              }

            //}
          });
        }
      }






      $scope.calcSampleData = function () {
        loadingRequest.show();
        var tableName = $scope.tableModel.selectedSrcTable.database + '.' + $scope.tableModel.selectedSrcTable.name;

        $scope.getSampleJobStatus(tableName, function () {
          TableExtService.doSample({
            projectName: $scope.projectModel.selectedProject,
            tableName: tableName,
            action: 'sample_job'
          }, {}, function () {
            loadingRequest.hide();
            SweetAlert.swal('', $scope.dataKylin.alert.collectStaticsSuccess, 'success');
          }, function (e) {
            var message;
            if (e.data && e.data.exception) {
              message = e.data.exception;
            } else {
              message = $scope.dataKylin.alert.error_info;
            }
            loadingRequest.hide();
            SweetAlert.swal('', message, 'error');
          })
        })

      }
      $scope.getSampleData = function () {
        TableExtService.getSampleInfo({tableName: $scope.tableModel.selectedSrcTable.database + '.' + $scope.tableModel.selectedSrcTable.name}, function (data) {
          var sampleData = [], specialData = [];
          if (data.sample_rows && data.sample_rows.length) {
            sampleData = sampleData.concat(VdmUtil.changeDataAxis(data.sample_rows, true));
          }
          $scope.specialData = data.columns_stats;
          $scope.sampleData = sampleData;
          $scope.last_modified = data.last_modified_time;
          $scope.total_rows = data.total_rows;
        })
      }

      $scope.$watch('tableModel.selectedSrcTable.name', function () {
        if ($scope.tableModel&&$scope.tableModel.selectedSrcTable.name) {
          $scope.getSampleData();
        }
      })
      $scope.getSampleJobStatus = function (tableName, callback) {
        TableExtService.getCalcSampleProgress({'tableName': tableName, 'action': 'job'}, function (data) {
          if (data.job_status == 'RUNNING' || data.job_status == 'PENDING') {
            SweetAlert.swal('', $scope.dataKylin.alert.hasCollectJob, 'info');
            loadingRequest.hide();
          } else if (typeof  callback == 'function') {
            callback()
          }
        }, function (e) {
          kylinCommon.error_default(e);
          loadingRequest.hide();
        })
      }

  });

