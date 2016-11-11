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

/**
 * Created by jiazhong on 2015/3/13.
 */

'use strict';

KylinApp.controller('ModelMeasuresCtrl', function ($scope, $modal,MetaModel,modelsManager, modelsEdit) {

  $scope.selectedColumns={};

  $scope.initMeasure = function () {
      $scope.availableTables=modelsManager.selectedModel.fact_table
      var cols2 = $scope.getColumnsByTable($scope.availableTables);
      $scope.usedMeasures=$scope.unique($scope.usedMeasures);
      var SelectAvailable = {};
      for (var k = 0; k < cols2.length; k++) {
          // Default not selected and not disabled.
          SelectAvailable[cols2[k].name] = {name:cols2[k].name,selected: false,disabled:false};
      }
      SelectAvailable.all=false;
      SelectAvailable.disabled=false;
      SelectAvailable.open=false;
      SelectAvailable.sortFlag = ''
      SelectAvailable.sortIcon = 'fa fa-unsorted';
      $scope.selectedColumns = SelectAvailable;
      angular.forEach(modelsManager.selectedModel.metrics, function (column) {
          $scope.selectedColumns[column].selected=true;
      });
      angular.forEach($scope.usedMeasures, function (column) {
          $scope.selectedColumns[column].disabled=true;
      });
      var all=true;
      var disabled=true;
      var open=false;
      angular.forEach($scope.selectedColumns,function(col){
          if(typeof col=="object"){
              if(col.selected==false){
                  all=false;
              }else{
                  open=true;
              }
              if(col.disabled==false){
                  disabled=false;
              }
          }
      });
      $scope.selectedColumns.disabled=disabled;
      $scope.selectedColumns.all=all;
      $scope.selectedColumns.open=open;
  }

  if ($scope.state.mode == 'edit') {
      $scope.initMeasure();
  }

  $scope.sort = function(){
      if($scope.selectedColumns.sortFlag === ''){$scope.selectedColumns.sortFlag = 'name';$scope.selectedColumns.sortIcon='fa fa-sort-asc';return;}
      if($scope.selectedColumns.sortFlag === 'name'){$scope.selectedColumns.sortFlag = '-name';$scope.selectedColumns.sortIcon='fa fa-sort-desc';return;}
      if($scope.selectedColumns.sortFlag === '-name'){$scope.selectedColumns.sortFlag = '';$scope.selectedColumns.sortIcon='';return;}
  }

  $scope.Change= function(name){
      if($scope.selectedColumns[name].selected==false){
          $scope.selectedColumns.all=false;
          modelsManager.selectedModel.metrics.splice(modelsManager.selectedModel.metrics.indexOf(name),1);
      }else{
          var all=true;
          angular.forEach($scope.selectedColumns,function(col){
              if(col.selected==false&&typeof col=="object"){
                  all=false;
              }
          });
          $scope.selectedColumns.all=all;
          modelsManager.selectedModel.metrics.push(name);
      }
  }

  $scope.ChangeAll= function(){
      angular.forEach($scope.selectedColumns,function(col){
          if($scope.usedMeasures.length>0){
              if(typeof col==="object"&&col.disabled==false){
                  var local=modelsManager.selectedModel.metrics.indexOf(col.name);
                  if($scope.selectedColumns.all==true&&col.selected==false){
                      col.selected=true;
                      modelsManager.selectedModel.metrics.push(col.name);
                  }
                  if($scope.selectedColumns.all==false&&col.selected==true){
                      col.selected=false;
                      modelsManager.selectedModel.metrics.splice(local,1);
                  }
              }
          }else{
              if(typeof col==="object"){
                  if($scope.selectedColumns.all==true){
                      if(col.selected == false){
                         col.selected=true;
                         modelsManager.selectedModel.metrics.push(col.name);
                      }
                  }else{
                      col.selected=false;
                      modelsManager.selectedModel.metrics.splice(0,1);
                  }
              }
          }
      });
  }

});
