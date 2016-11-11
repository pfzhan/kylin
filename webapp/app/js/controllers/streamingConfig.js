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

KylinApp.controller('streamingConfigCtrl', function ($scope,StreamingService, $q, $routeParams, $location, $window, $modal, MessageService, CubeDescService, CubeService, JobService, UserService, ProjectService, SweetAlert, loadingRequest, $log, modelConfig, ProjectModel, ModelService, MetaModel, modelsManager, cubesManager, TableModel, $animate,StreamingModel,kylinCommon) {

  $scope.tableModel = TableModel;

  if($scope.state.mode=='view') {
    $scope.streamingMeta = StreamingModel.createStreamingConfig();
    $scope.kafkaMeta = StreamingModel.createKafkaConfig();
  }

  if($scope.state.mode=='edit'&& $scope.state.target=='kfkConfig' && $scope.state.tableName){
    StreamingService.getConfig({table:$scope.state.tableName}, function (configs) {
      if(!!configs[0]&&configs[0].name.toUpperCase() == $scope.state.tableName.toUpperCase()){
        $scope.updateStreamingMeta(configs[0]);
        StreamingService.getKfkConfig({kafkaConfigName:$scope.streamingMeta.name}, function (streamings) {
          if(!!streamings[0]&&streamings[0].name.toUpperCase() == $scope.state.tableName.toUpperCase()){
            $scope.updateKafkaMeta(streamings[0]);
          }
        })
      }
    })
  }


  $scope.addCluster = function () {
    $scope.kafkaMeta.clusters.push(StreamingModel.createKafkaCluster());
  };

  $scope.removeCluster = function(cluster){

    SweetAlert.swal({
      title: '',
      text: $scope.dataKylin.alert.tip_to_remove_cluster,
      type: '',
      showCancelButton: true,
      confirmButtonColor: '#DD6B55',
      confirmButtonText: "Yes",
      closeOnConfirm: true
    }, function(isConfirm) {
      if(isConfirm) {
        var index = $scope.kafkaMeta.clusters.indexOf(cluster);
        if (index > -1) {
          $scope.kafkaMeta.clusters.splice(index, 1);
        }
      }

    })
  }

  $scope.addBroker = function (cluster,broker) {
    //$scope.modelsManager.selectedModel = model;
    cluster.newBroker=(!!broker)?broker:StreamingModel.createBrokerConfig();
  };

  $scope.removeNewBroker = function (cluster){
    delete cluster.newBroker;
  }

  $scope.removeElement = function (cluster, element) {
    var index = cluster.brokers.indexOf(element);
    if (index > -1) {
      cluster.brokers.splice(index, 1);
    }
  };

  $scope.saveNewBroker = function(cluster){
    if (cluster.brokers.indexOf(cluster.newBroker) === -1) {
      cluster.brokers.push(cluster.newBroker);
    }
    delete cluster.newBroker;
  }

  $scope.clearNewBroker = function(cluster){
    delete cluster.newBroker;
  }


  $scope.streamingTsColUpdate = function(){
    if(!$scope.streamingCfg.parseTsColumn){
      $scope.streamingCfg.parseTsColumn = ' ';
    }
    $scope.kafkaMeta.parserProperties = "tsColName="+$scope.streamingCfg.parseTsColumn;
  }

  $scope.$watch('tableModel.selectedSrcTable', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    //view model
    if($scope.state.mode == 'view' && $scope.tableModel.selectedSrcTable.source_type==1){
      $scope.reloadMeta();
    }

  });


  $scope.$on('StreamingConfigEdited', function (event) {
    $scope.reloadMeta();
  });

  $scope.reloadMeta = function(){
    var table = $scope.tableModel.selectedSrcTable;
    var streamingName = table.database+"."+table.name;
    $scope.streamingMeta = {};
    $scope.kafkaMeta = {};
    StreamingService.getConfig({table:streamingName}, function (configs) {
      if(!!configs[0]&&configs[0].name.toUpperCase() == streamingName.toUpperCase()){
        $scope.streamingMeta = configs[0];
        StreamingService.getKfkConfig({kafkaConfigName:$scope.streamingMeta.name}, function (streamings) {
          if(!!streamings[0]&&streamings[0].name.toUpperCase() == streamingName.toUpperCase()){
            $scope.kafkaMeta = streamings[0];
          }
        })
      }
    })
  }

});
