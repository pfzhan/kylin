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

/**
 *MetaModel will manage model info of cube
 */
KylinApp.service('MetaModel',function(TableModel, StringHelper){

    //data model when edit model
    this.model={
        name: null,
        description:null,
        fact_table: null,
        lookups: [],
        filter_condition:null,
        capacity:null,
        dimensions:[],
        metrics:[],
        "partition_desc" : {
            "partition_date_column" : null,
            "partition_date_start" : null,
            "partition_type" : 'APPEND',
            "partition_time_column" : null,
            "partition_time_start" : null
        },
        last_modified:0,
        aliasColumnMap:{}
    };


    this.setMetaModel =function(model){
        var _model = {};
        _model.name = model.name;
        _model.description = model.description;
        _model.fact_table = model.fact_table;
        _model.lookups =model.lookups;
        _model.filter_condition = model.filter_condition;
        _model.capacity = model.capacity;
        _model.dimensions = model.dimensions;
        _model.metrics = model.metrics;
        _model.partition_desc = model.partition_desc;
        _model.last_modified = model.last_modified;
        _model.aliasColumnMap={};
        angular.forEach(TableModel.tableColumnMap,function(tables,tableName){
          if(_model.fact_table==tableName){
           var rootFactTable=StringHelper.removeNameSpace(_model.fact_table);
           angular.forEach(tables,function(column,columnName){
              _model.aliasColumnMap[rootFactTable+"."+columnName]=column;
           });
          }
          angular.forEach(_model.lookups,function(joinTable){
            if(joinTable.table==tableName){
              angular.forEach(tables,function(column,columnName){
                if(!joinTable.alias){
                  joinTable.alias=StringHelper.removeNameSpace(joinTable.table);
                }
                _model.aliasColumnMap[joinTable.alias+"."+columnName]=column;
              });
            }
          });
        });
        this.model = _model;
        return this.model;
    };

    this.initModel = function(){
        this.model = this.createNew();
    }

    this.getMetaModel = function(){
        return this.model;
    };

    this.setFactTable = function(fact_table) {
        this.model.fact_table =fact_table;
    };

    this.createNew = function () {
        var metaModel = {
            name: '',
            description:'',
            fact_table: '',
            lookups: [],
            filter_condition:'',
            capacity:'MEDIUM',
            dimensions:[],
            metrics:[],
            "partition_desc" : {
                "partition_date_column" : null,
                "partition_date_start" : null,
                "partition_type" : 'APPEND',
                "partition_date_format":'yyyy-MM-dd',
                "partition_time_column" : null,
                "partition_time_start" : null
            },
            last_modified:0
        };

        return metaModel;
    }
})
