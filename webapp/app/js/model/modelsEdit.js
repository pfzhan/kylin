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

KylinApp.service('modelsEdit',function(ModelService,CubeService,$q,AccessService,ProjectModel,$log){
    var _this = this;
    this.models=[];
    this.modelNameList = [];

    //tracking models loading status
    this.loading = false;
    this.selectedModel={};

    this.cubeModel={};
    this.cubeSelected = false;

    //list models and complemete cube,access info
    this.list = function(queryParam){

        _this.loading = true;
        var defer = $q.defer();
        var cubeDetail = [];
        var modelPermission = [];
        ModelService.list(queryParam, function (_models) {
            //_this.removeAll();
            _this.modelNameList=[];
            angular.forEach(_models, function (model, index) {
                $log.info("Add model permission info");
                if(model.uuid){
                  modelPermission.push(
                  AccessService.list({type: "DataModelDesc", uuid: model.uuid}, function (accessEntities) {
                      model.accessEntities = accessEntities;
                      //if no owner in metadata, will get from acl
                    try{
                      if(!model.owner){
                        model.owner = accessEntities[0].sid.principal;
                      }
                    } catch(error){
                      //$log.error("No acl info. Model: %s",model.name);
                    }
                  }).$promise
                  )
                }

                //$log.info("Add cube info to model ,not detail info");
                cubeDetail.push(
                    CubeService.list({modelName:model.name}, function (_cubes) {
                    model.cubes = _cubes;
                    }).$promise
                );

              _this.modelNameList.push(model.name);

                model.project = ProjectModel.getProjectByCubeModel(model.name);
            });
            $q.all(cubeDetail,modelPermission).then(
                function(result){
                    _models = _.filter(_models,function(models){return models.name!=undefined});
                    _this.models = _models;
                    _this.loading = false;
                    defer.resolve(_this.models);
                }
            );
        },function(){
            defer.reject("Failed to load models");
        });
        return defer.promise;

    };

    this.removemodels = function(models){
        var modelsIndex = _this.models.indexOf(models);
        if (modelsIndex > -1) {
            _this.models.splice(modelsIndex, 1);
        }
    }

    this.getModel = function(modelName){
      return  _.find(_this.models,function(unit){
            return unit.name == modelName;
        })
    }


    this.getModels = function(){
        return _this.models;
    }

    this.setSelectedModel = function(value){
        this.selectedModel = value;
    }


    this.getModelByCube = function(cubeName){
        return  _.find(_this.models,function(model){
            return _.some(model.cubes,function(_cube){
                return _cube.name == cubeName;
            });
        })
    }

    this.removeAll = function(){
        _this.models = [];
        _this.selectedModel = {};
        _this.modelNameList = [];
    };

    this.listAccess = function (entity, type) {
        var defer = $q.defer();

        entity.accessLoading = true;
        AccessService.list({type: type, uuid: entity.uuid}, function (accessEntities) {
            entity.accessLoading = false;
            entity.accessEntities = accessEntities;
            defer.resolve();
        });

        return defer.promise;
    };

    this.getTableDesc = function(tableName){
      var tags = [];
      if(!tableName){
        return tags;
      }
      for(var i =0;i<_this.models.length;i++){
        var _model = _this.models[i];
        if(_model.fact_table.toUpperCase()==tableName.toUpperCase()){
          tags.push("FACT");
          break;
        }
        for(var j=0;j<_model.lookups.length;j++){
          var _lookup = _model.lookups[j];
          if(tableName.toUpperCase() == _lookup.table.toUpperCase()){
            tags.push("LOOKUP");
            tags.push(_lookup.join.type.toUpperCase());
            return tags;
          }

        }


      }
      return tags;
    }

    this.getColumnDesc = function(tableName, columnName){
      if(!tableName || !columnName){
        return tags;
      }
      var tags = [];
      var upperTableName = tableName.toUpperCase();
      var upperColumnName = columnName.toUpperCase();


      for(var i=0;i<_this.models.length;i++){
        var _model = _this.models[i];
        for(var j=0;j<_model.dimensions.length;j++){
          var _dim = _model.dimensions[j];
          if(_dim.table.toUpperCase() == upperTableName && _dim.columns.indexOf(upperColumnName)!=-1 ){
            tags.push("D");
          }
        }

        if(tableName.toUpperCase() == _model.fact_table && _model.metrics.indexOf(upperColumnName)!=-1 ){
          tags.push("M");
        }

        for(var k=0;k<_model.lookups.length;k++){
          var _lookup = _model.lookups[k];
          if(upperTableName == _model.fact_table.toUpperCase()){
            if(_lookup.join.foreign_key.indexOf(upperColumnName)!=-1){
              tags.push("FK");
            }
          }
          else if(upperTableName == _lookup.table && _lookup.join.primary_key.indexOf(upperColumnName)!=-1 ){
            tags.push("PK");
          }

        }

      }

      return tags;
    }


});
