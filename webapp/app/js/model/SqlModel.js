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

KylinApp.service('SqlModel',function(ModelService,CubeService,$q,AccessService,ProjectModel,$log){

    //TO DO COUNT_DISTINCT,TOPN
    var _this = this;

  /*
   * [SEG1] SELECT
   *          [SEG2]COUNT(*),SUM|MIN|MAX(FACT_TABLE.M1) - SEG6
   *              [SEG3]FROM FACT_TABLE
   *              [SEG4] INNER JOIN LOOKUP_TABLE
   *                        ON FACT_TABLE.FK1 = LOOKUP_TABLE.PK1
   *                        AND FACT_TABLE.FK2 = LOOKUP_TABLE.PK1
   *                        .....
   *              [SEG5]GROUP BY
   *              [SEG6]FACT_TABLE.DIM1,LOOKUP_TABLE.DIM2
   *              [SEG7]ORDER BY ...
   *
   */

    this.model = {};
    //mea_expression,mea_type,mea_value
    this.tableMeasures={};
    //array column->dimension
    this.columnDimensions = {};

    //[{name,table}]
    this.dimensions = [];
    this.selectedDimensions = [];
    this.tableNameMap = {};
    this.tableAliasMap = {};

    /*
     *
     * {
     *  mea_expression:expression,
     *  mea_type:type,
     *  mea_value:column,
     *  mea_display:expression+'('+column+')',
     *  mea_next_parameter:{..}
     *  }
     *
     *
     * @measures, all available measures
     * @selectedMeasures, all available measures
     *
     */

    this.measures = [];
    this.selectedMeasures = [];

    //tableModel[fact][lookup].join
    this.tableModelJoin = {}

    /*
     * will not include all tables, only tables available
     */
    this.factTable = "";
    this.lookupTables = [];

    this.SEG1,this.SEG2,this.SEG3,this.SEG4,this.SEG5,this.SEG6,this.SEG7,this.SQL;

    /*
     *
     * @Generate Order
     *     sql seg init
     *     used tables init - set used fact table and lookups
     *     table join map init - set fact-lookup join map entity
     *     table name map init - special character convert & alias
     *
     *
     */

    this.init = function(){

      _this.initSql();
      _this.initTables();
      _this.initTableJoin();
      _this.initTableNameMap();

      _this.getSeg4();
      _this.getSeg6();
      _this.getSeg3();
      _this.getSeg2();

    }

    this.initSql = function(){
      _this.SEG1 = "SELECT \n";
      _this.SEG2 = " ";
      _this.SEG3 = " ";
      _this.SEG4 = " ";
      _this.SEG5 = " GROUP BY \n";
      _this.SEG6 = " ";
      _this.SEG7 = " ORDER BY";
      _this.SQL="";
    }

    //input tables here and gen sql
    this.getSql = function(){
      _this.SQL = _this.SEG1+_this.SEG2+_this.SEG3+_this.SEG4+_this.SEG5+_this.SEG6;
      //add order by for topn
      if(_this.selectedMeasures.length==1&&_this.selectedMeasures[0].mea_expression == "TOP_N") {
        _this.SQL+=_this.SEG7;
      }

      if(!_this.selectedMeasures.length && !_this.selectedDimensions.length){
        _this.SQL = "";
      }

      return _this.SQL;
    }

    /*
     * set all used lookup tables and fact_table
     * topN will set table by next parameter
     */
    this.initTables = function(){
      _this.factTable = _this.model.fact_table;
      _this.lookupTables = [];
      for(var i=0;i<_this.selectedDimensions.length;i++){
        var _dimension = _this.selectedDimensions[i];
        if(_dimension.table!==_this.factTable && _this.lookupTables.indexOf(_dimension.table)==-1){
          _this.lookupTables.push(_dimension.table);
        }
      }

      //for topN
      if(_this.selectedMeasures.length==1&&_this.selectedMeasures[0].mea_expression == "TOP_N"){
        var measure = _this.selectedMeasures[0];
        var groupByColumn = measure.mea_next_parameter.value;
        var dimension =_this.columnDimensions[groupByColumn];

        //add lookup table info from topN
        if(dimension.table!==_this.factTable && _this.lookupTables.indexOf(dimension.table)==-1){
          _this.lookupTables.push(dimension.table);
        }

        //add dimension in topN
        var dimensionExist = false;
        for(var i=0;i<_this.selectedDimensions.length;i++){
          var _dimension = _this.selectedDimensions[i];
          if(_dimension.name == dimension.name){
            dimensionExist = true;
            break;
          }
        }

        if(!dimensionExist){
          _this.selectedDimensions.push(dimension);
        }
      }

    }

    //DEFAULT.TABLE_NAME->"DEFAULT".TABLE_NAME and alias init
    _this.initTableNameMap = function(){
      var tables = [_this.model.fact_table];

      for(var i=0;i<_this.model.lookups.length;i++) {
        var lookup = _this.model.lookups[i].table;
        tables.push(lookup);
      }

      for(var i =0;i<tables.length;i++ ){
        var table = tables[i];
        var index = table.indexOf('.');

        var newTableName = table;
        if(index != -1){
          var TABLE_DB = table.substr(0, index);
          var TABLE_NAME = table.substr(index + 1, table.length);
          newTableName = "\"" + TABLE_DB + "\"" + "." + TABLE_NAME;
        }
        _this.tableNameMap[table] = newTableName;
      }

      var ALIAS_PREFIX="t";
      for(var i =0;i<tables.length;i++ ){
        var table = tables[i];
        _this.tableAliasMap[table] = ALIAS_PREFIX+i;
      }

    }

    //preCondition - model ready
    this.initTableJoin = function(){
      if(!_this.tableModelJoin[_this.factTable]){
        _this.tableModelJoin[_this.factTable] ={};
      }

      for(var i=0;i<_this.model.lookups.length;i++){
        var lookup = _this.model.lookups[i];
        _this.tableModelJoin[_this.factTable][lookup.table] = {};
        _this.tableModelJoin[_this.factTable][lookup.table]=lookup.join;
      }
    }


    this.getSeg1 = function(){
      return _this.SEG1;
    }

    //pre condition SEG6
    this.getSeg2 = function(){
      var sqlSeg = " ";

      for(var i=0;i<_this.selectedMeasures.length;i++){
        var split = ", ";

        if(i==_this.selectedMeasures.length-1){
          //if no group by
          if(!_this.selectedDimensions.length){
            split = " \n";
          }else{
            split = ", ";
          }
        } else{
          split = ", ";
        }


        var measure = _this.selectedMeasures[i];
        if(measure.mea_type.toUpperCase()=="COLUMN"){
          if(measure.mea_expression == "COUNT_DISTINCT"){
            sqlSeg += "COUNT " +"("+"DISTINCT "+_this.tableAliasMap[_this.factTable]+"."+measure.mea_value+")"+split;
          }else if(measure.mea_expression == "TOP_N"){
            sqlSeg += "SUM " +"("+_this.tableAliasMap[_this.factTable]+"."+measure.mea_value+")"+split;
            _this.SEG7 = " ORDER BY "+"SUM " +"("+_this.tableAliasMap[_this.factTable]+"."+measure.mea_value+")";
          }
          else{
            sqlSeg += measure.mea_expression +"("+_this.tableAliasMap[_this.factTable]+"."+measure.mea_value+")"+split;
          }
        }else if(measure.mea_type.toUpperCase()=="CONSTANT"){
          sqlSeg += measure.mea_expression +"("+measure.mea_value+")"+split;
        }
      }
      _this.SEG2 = sqlSeg +_this.SEG6;
    }

    this.getSeg3 = function(){
      var sqlSeg = " FROM ";
      var factTable = _this.factTable;
      sqlSeg += " "+_this.tableNameMap[factTable] +" AS "+_this.tableAliasMap[factTable]+" \n";
      _this.SEG3 = sqlSeg;
    }

    this.getSeg4 = function(){
      var factFullName = _this.factTable;
      var lookups = _this.lookupTables;
      var sqlSeg = " ";
      var lookupFullName ="";

      for(var i=0;i<lookups.length;i++){
          lookupFullName = lookups[i];
         var joinEntity = _this.tableModelJoin[factFullName][lookupFullName];
         var joinType = joinEntity.type;
         var joinSeg = " "+joinType.toUpperCase()+" JOIN "+_this.tableNameMap[lookupFullName] +" AS "+_this.tableAliasMap[lookupFullName]+" \n";

        sqlSeg+=joinSeg;

        var onSeg = " ON ";
        for(var j=0;j<joinEntity.foreign_key.length;j++){
          var fk = joinEntity.foreign_key[j];
          var pk = joinEntity.primary_key[j];
          if(j==0){
            onSeg += " "+_this.tableAliasMap[factFullName]+"."+fk +" = " + _this.tableAliasMap[lookupFullName]+"."+pk+" \n";
          }else{
            onSeg += " AND "+_this.tableAliasMap[factFullName]+"."+ fk +" = " + _this.tableAliasMap[lookupFullName]+"."+pk+" \n";
          }

        }
        sqlSeg += onSeg;
      }

      _this.SEG4 = sqlSeg;

    }

    this.getSeg6 = function(){
      var sqlSeg = " ";

      for(var i=0;i<_this.selectedDimensions.length;i++){
        var split = i==_this.selectedDimensions.length-1?" \n":", ";
        var dimension = _this.selectedDimensions[i];
        sqlSeg += _this.tableAliasMap[dimension.table]+"."+dimension.name+split;
      }
      if(!_this.selectedDimensions.length){
        _this.SEG5 = "";
      }
      _this.SEG6 = sqlSeg;
    }


    this.isSqlAvailable = function(){
      if(_this.selectedDimensions.length || _this.selectedMeasures.length){
        return true;
      }
      return false;
    }


    this.removeDimensionByIndex = function(arr,index){
      _this.removeItemsByIndex(arr,index);

      //remove topN measure when remove topN last dimension
      if(!_this.selectedDimensions.length && _this.selectedMeasures.length == 1 && _this.selectedMeasures[0].mea_expression == "TOP_N"){
        _this.selectedMeasures = [];
      }
      _this.init();
      _this.getSql();
    }

    /*
     * mandatory group by column can't be removed
     */
    this.removeMeasureByIndex = function(arr,index){
      _this.removeItemsByIndex(arr,index);
      _this.init();
      _this.getSql();
    }

    //remove dimension || measure
    this.removeItemsByIndex = function(arr,index){
      arr.splice(index,1);
    }

    this.measureOnDrop = function(){

      //last index duplicate ?
      var measures = _this.selectedMeasures;
      var newAppendMeasure = measures[measures.length-1];

      //do not add TOP_N when other measure exist
      if(newAppendMeasure.mea_expression == "TOP_N" && measures.length>1){
        _this.selectedMeasures.splice(measures.length-1,1);
      }

      //do not add measure when there's a TOP_N measure
      if(measures[0].mea_expression == "TOP_N" && measures.length>1){
        _this.selectedMeasures.splice(measures.length-1,1);
      }

      var eleCount=0;
      for(var i=0;i<measures.length;i++){
        var _measure = measures[i];
        if(_measure.mea_value == newAppendMeasure.mea_value && _measure.mea_expression == newAppendMeasure.mea_expression){
          eleCount ++;
        }
      }

      if(eleCount>1){
        _this.selectedMeasures.splice(measures.length-1,1);
      }

      _this.init();
      _this.getSql();
    }

  this.dimensionOnDrop = function(){

    //last index duplicate ?
    var dimensions = _this.selectedDimensions;
    var newAppendDimension = dimensions[dimensions.length-1];
    var eleCount=0;
    for(var i=0;i<dimensions.length;i++){
      var _dimension = dimensions[i];
      if(_dimension.name == newAppendDimension.name){
        eleCount ++;
      }
    }

    if(eleCount>1){
      _this.selectedDimensions.splice(dimensions.length-1,1);
    }

    _this.init();
    _this.getSql();
  }

});
