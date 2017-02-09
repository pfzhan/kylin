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

KylinApp.service('TableModel', function(ProjectModel,$q,TableService,$log,StringHelper,EncodingService,tableConfig) {
    var _this = this;
    //tracking loading status
    var loading = false;
   //for tables in cubeDesigner
    this.selectProjectTables = [];
    this.tableColumnMap={};
    this.columnNameTypeMap = {};
    this.columnTypeEncodingMap={}

    this.initTables = function(){
        this.selectProjectTables = [];
    }

    this.addTable = function(table){
        this.selectProjectTables.push(table);
    }

    this.setSelectedProjectTables = function(tables) {
        this.selectProjectTables = tables;
    }


  // for load table page
    this.selectedSrcDb = [];
    this.selectedSrcTable = {};

    this.init = function(){
      this.selectedSrcDb = [];
      this.selectedSrcTable = {};
    };
    this.getcolumnNameTypeMap=function(callback){
      var param = {
        ext: true,
        project:ProjectModel.selectedProject
      };
      if(angular.equals({}, _this.columnNameTypeMap)) {
        TableService.list(param, function (tables) {

          angular.forEach(tables, function (table) {
            angular.forEach(table.columns, function (column) {
              _this.columnNameTypeMap[table.name+'.'+column.name] = column.datatype;
            });
          });
          if(typeof  callback=='function'){
            callback(_this.columnNameTypeMap);
          }
        });
      }else{
        if(typeof  callback=='function'){
          callback(_this.columnNameTypeMap);
        }
      }
    }
    this.getColumnTypeEncodingMap=function(){
      var _this=this;
      var defer = $q.defer();
      if(!angular.equals({},_this.columnTypeEncodingMap)){
        defer.resolve(_this.columnTypeEncodingMap);
      }
      EncodingService.getEncodingMap({},{},function(result){
        if(result&&result.data){
          _this.columnTypeEncodingMap=result.data;
        }else{
          _this.columnTypeEncodingMap=tableConfig.columnTypeEncodingMap;
        }
         defer.resolve(_this.columnTypeEncodingMap);
      },function(){
         _this.columnTypeEncodingMap=tableConfig.columnTypeEncodingMap;
         defer.resolve(_this.columnTypeEncodingMap);
      })

      return defer.promise;
    }
    this.aceSrcTbLoaded = function (forceLoad) {
        _this.selectedSrcDb = [];
        _this.loading = true;

        _this.selectedSrcTable = {};
        var defer = $q.defer();

        var param = {
            ext: true,
            project:ProjectModel.selectedProject
        };

        if(!ProjectModel.selectedProject||!ProjectModel.isSelectedProjectValid()){
            defer.resolve();
            return defer.promise;
        }

      TableService.list(param, function (tables) {
            var tableMap = [];
            angular.forEach(tables, function (table) {
                var tableName=table.database+"."+table.name;
                var tableData = [];
               _this.tableColumnMap[tableName]={};
                if (!tableMap[table.database]) {
                    tableMap[table.database] = [];
                }
               angular.forEach(table.columns, function (column) {
                    _this.tableColumnMap[tableName][column.name]={
                        name:column.name,
                        datatype:column.datatype,
                        cardinality:table.cardinality[column.name],
                        comment:column.comment};
                     if(table.cardinality[column.name]) {
                        column.cardinality = table.cardinality[column.name];
                    }else{
                        column.cardinality = null;
                    }
                    column.id = parseInt(column.id);
                  _this.columnNameTypeMap[tableName+'.'+column.name] = column.datatype;
                });
                tableMap[table.database].push(table);
            });

//                Sort Table
            for (var key in  tableMap) {
                var obj = tableMap[key];
                obj.sort(_this.innerSort);
            }

            _this.selectedSrcDb = [];
            for (var key in  tableMap) {

                var tables = tableMap[key];
                var _db_node = {
                    label:key,
                    fullName:key,
                    data:tables,
                    onSelect:function(branch){
                        $log.info("db "+key +"selected");
                    }
                }

                var _table_node_list = [];
                angular.forEach(tables,function(_table){

                        var table_icon="fa fa-table";
                        if(_table.source_type==1){
                          table_icon="fa fa-th"
                        }

                        var _table_node = {
                            label:_table.name,
                            data:_table,
                            fullName:key+'.'+_table.name,
                            icon:table_icon,
                            onSelect:function(branch){
                                // set selected model
                                _this.selectedSrcTable = branch.data;
                            }
                        }

                        var _column_node_list = [];
                        angular.forEach(_table.columns,function(_column){
                            _column_node_list.push({
                                    label:_column.name+"  ("+_column.datatype+")",
                                    data:_column,
                                    onSelect:function(branch){
                                        // set selected model
//                                        _this.selectedSrcTable = branch.data;
                                        _this.selectedSrcTable.selectedSrcColumn = branch.data;

                                    }
                                });
                        });
                         _table_node.children =_column_node_list;
                        _table_node_list.push(_table_node);

                        _db_node.children = _table_node_list;
                    }
                );

                _this.selectedSrcDb.push(_db_node);
            }
            _this.loading = false;
            defer.resolve();
        },function(e){
          defer.reject("Failed to load tables, please check system log for details.");
        });

        return defer.promise;
    };

    this.getColumnType = function(_column,_table){
        var columns = _this.getColumnsByTable(_table);
        var type;
        angular.forEach(columns,function(column){
            if(_column === column.name){
                type = column.datatype;
                return;
            }
        });
        return type;
    };

    this.getColumnsByTable = function (tableName) {
        var temp = [];
        angular.forEach(_this.selectProjectTables, function (table) {
            if (table.database+'.'+table.name == tableName) {
                temp = table.columns;
            }
        });
        return temp;
    };

    this.innerSort =function(a, b) {
        var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
        if (nameA < nameB) //sort string ascending
            return -1;
        if (nameA > nameB)
            return 1;
        return 0; //default return value (no sorting)
    };




    //new tableModel for snow======

   //table detail load install
   this.tableDetails;
   this.tableOriginalData;
   this.initTableData=function(callback,project){
      var that=this;
      //if(!that.tableDetails){
        that.tableDetails=[];
        var param = {
          ext: true,
          project:project||ProjectModel.selectedProject
        };
        TableService.list(param, function (tables) {
          that.tableOriginalData=tables;
          angular.forEach(tables, function (table) {
             var columnsLen=table.columns&&table.columns.length||0;
             for(var i=0;i<columnsLen;i++){
               var columnsObj={};
               columnsObj.database=table.database;
               columnsObj.table=StringHelper.removeNameSpace(table.name);
               columnsObj.fullTable=columnsObj.database+'.'+columnsObj.table;
               columnsObj.columnName=StringHelper.removeNameSpace(table.columns[i].name);
               columnsObj.columnType=table.columns[i].datatype;
               columnsObj.fullColumnName=columnsObj.table+'.'+columnsObj.columnName;
               that.tableDetails.push(columnsObj);
             }
          })
          if(typeof callback=='function'){
            callback(that.tableDetails)
          }
        })

   }
   /*
   *filterName example
   * database.tablename.columnname
   * tablename.columnname
   * columnname
   * */
   this.getColumnDetail=function(filterName){
      if(!filterName){
        return null;
      }
      var len=this.tableDetails&&this.tableDetails.length||0;
      for(var i=0;i<len;i++){
        var splitList=filterName.split('.');
        if(splitList.length==1){
          if(this.tableDetails[i].columnName==splitList[0]){
            return this.tableDetails[i];
          }
        }else if(splitList.length==2){
          if(this.tableDetails[i].table==splitList[0]&&this.tableDetails[i].columnName==splitList[1]){
            return this.tableDetails[i];
          }
        }else if(splitList.length==3){
          if(this.tableDetails[i].database==splitList[1]&&this.tableDetails[i].table==splitList[2]&&this.tableDetails[i].columnName==splitList[3]){
            return this.tableDetails[i];
          }
        }
      }
      return null;
   },
   /*
    *filterName example
    * database.tablename
    * database
    * */
   this.getTableDatail=function(filterName){
      var filterList=filterName.split('.'),result=[];
      for(var i= 0,len=this.tableOriginalData&&this.tableOriginalData.length||0;i<len;i++){
        if(filterList){
          if(filterList.length==1&&this.tableOriginalData[i].database==filterList[0]){
            result.push(this.tableOriginalData[i]);
          }else if(filterList.length==2&&this.tableOriginalData[i].database==filterList[0]&&this.tableOriginalData[i].name==filterList[1]){
            result.push(this.tableOriginalData[i]);
          }
        }
      }
     return result;
   },
   //获取database列表
    this.getDataBaseList=function(){
        var obj={},databaseList=[];
        for(var i= 0,len=this.tableOriginalData&&this.tableOriginalData.length||0;i<len;i++){
            obj[this.tableOriginalData[i].database]=1;
        }
        for(var i in obj){
            databaseList.push(i);
        }
        return databaseList;
    }
});

