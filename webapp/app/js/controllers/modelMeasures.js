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

KylinApp.controller('ModelMeasuresCtrl', function ($scope, $modal,MetaModel,modelsManager, modelsEdit, TableModel, VdmUtil) {

  $scope.selectedColumns={};
  $scope.availableFactTables = [];
  $scope.selectedFactTables = {};
  $scope.initMeasureEdit = function () {
    $scope.availableFactTables.push(VdmUtil.removeNameSpace($scope.modelsManager.selectedModel.fact_table));
    var joinTable = $scope.modelsManager.selectedModel.lookups;
    for (var j = 0; j < joinTable.length; j++) {
      if(joinTable[j].kind=='FACT'){
        $scope.availableFactTables.push(joinTable[j].alias);
        }
    }
    angular.forEach($scope.modelsManager.selectedModel.metrics,function(metric){
      $scope.selectedFactTables[VdmUtil.getNameSpace(metric)]=$scope.selectedFactTables[VdmUtil.getNameSpace(metric)]||[];
      $scope.selectedFactTables[VdmUtil.getNameSpace(metric)].push(metric);
    });

    for (var j = 0; j < $scope.availableFactTables.length; j++) {
      var cols2 = $scope.getColumnsByAlias($scope.availableFactTables[j]);
      // Initialize selected available.
      var SelectAvailable = {};
      for (var k = 0; k < cols2.length; k++) {
      // Default not selected and not disabled.
        SelectAvailable[cols2[k].name] = {name:cols2[k].name,selected: false,disabled:false};
      }
      SelectAvailable.disabled=false;
      SelectAvailable.open=true;
      SelectAvailable.sortFlag = ''
      SelectAvailable.sortIcon = 'fa fa-unsorted';
      $scope.selectedColumns[$scope.availableFactTables[j]] = SelectAvailable;
    }
    if(!$scope.initStatus.measures){
      $scope.initStatus.measures=true;
      angular.forEach(TableModel.selectProjectTables,function(table){
        angular.forEach(table.columns,function(column){
          if(column.kind=="measure"){
            angular.forEach($scope.availableFactTables,function(alias){
              if($scope.aliasTableMap[alias]==table.name){
                modelsManager.selectedModel.metrics.push(alias+"."+column.name);
              }
            });
          }
        });
      });
    }
    angular.forEach(modelsManager.selectedModel.metrics, function (column) {
      $scope.selectedColumns[VdmUtil.getNameSpace(column)][VdmUtil.removeNameSpace(column)].selected=true;
    });
    angular.forEach($scope.usedMeasuresCubeMap, function (column,columnName) {
      $scope.selectedColumns[VdmUtil.getNameSpace(columnName)][VdmUtil.removeNameSpace(columnName)].disabled=true;
      angular.forEach(column,function(cube){
        $scope.usedMeasuresCubeMap[columnName]=$scope.unique(column);
      });
    });

    angular.forEach($scope.selectedColumns,function(value,table){
      var all=true;
      var disabled=true;
      angular.forEach(value,function(col){
        if(typeof col=="object"){
          if(col.selected==false){
            all=false;
          }
          if(col.disabled==false){
            disabled=false;
          }
        }
      });
      $scope.selectedColumns[table].disabled=disabled;
      $scope.selectedColumns[table].all=all;
    });
     //   var column=$scope.getColumnsByTable(modelsManager.selectedModel.fact_table);
  };

  $scope.initMeasureView = function () {
     $scope.availableFactTables.push(VdmUtil.removeNameSpace($scope.model.fact_table));
     var joinTable = $scope.model.lookups ;
     for (var j = 0; j < joinTable.length; j++) {
       if(joinTable[j].kind=='FACT'){
         $scope.availableFactTables.push(joinTable[j].alias);
         }
     }
    angular.forEach($scope.model.metrics,function(metric){
      $scope.selectedFactTables[VdmUtil.getNameSpace(metric)]=$scope.selectedFactTables[VdmUtil.getNameSpace(metric)]||[];
      $scope.selectedFactTables[VdmUtil.getNameSpace(metric)].push(VdmUtil.removeNameSpace(metric));
    });
  }

  if ($scope.state.mode == 'edit') {
      $scope.initMeasureEdit();
  }else{
      $scope.initMeasureView();
  }

    $scope.Change= function(table,name,index){
       if($scope.selectedColumns[table][name].selected==false){
          $scope.selectedColumns[table].all=false;
           modelsManager.selectedModel.metrics.splice(modelsManager.selectedModel.metrics.indexOf(table+"."+name),1);
       }else{
          var all=true;
          angular.forEach($scope.selectedColumns[table],function(col){
             if(col.selected==false&&typeof col=="object"){
                 all=false;
             }
          });
          $scope.selectedColumns[table].all=all;
          modelsManager.selectedModel.metrics.push(table+"."+name);
       }
    }

    $scope.ChangeAll= function(table,index){
        angular.forEach($scope.selectedColumns[table],function(col){
            if($scope.cubesLength>0){
                if(typeof col==="object"&&col.disabled==false){
                    var local=modelsManager.selectedModel.metrics.indexOf(table+"."+col.name);
                    if($scope.selectedColumns[table].all==true&&col.selected==false){
                        col.selected=true;
                        modelsManager.selectedModel.metrics.push(table+"."+col.name);
                    }
                    if($scope.selectedColumns[table].all==false&&col.selected==true){
                        col.selected=false;
                        modelsManager.selectedModel.metrics.splice(local,1);
                    }
                }
            }else{
                if(typeof col==="object"){
                    if($scope.selectedColumns[table].all==true){
                        if(col.selected == false){
                           col.selected=true;
                           modelsManager.selectedModel.metrics.push(table+"."+col.name);
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
