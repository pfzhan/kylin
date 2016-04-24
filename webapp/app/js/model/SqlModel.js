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
   *
   */
    //mea_expression,mea_type,mea_value
    this.tableMeasures={};
    //array
    this.tableDimensions = {};

    //[{name,table}]
    this.dimensions = [];
    this.selectedDimensions = [];

    //[{expresson,column}]
    this.measures = [];
    this.selectedMeasures = [];

    //tableModel[fact][lookup].join
    this.tableModelJoin = {}

    this.rightSql = false;
    this.factTable = "";
    this.lookupTables = [];

    this.SEG1 = "SELECT \n";
    this.SEG2 = " ";
    this.SEG3 = " ";
    this.SEG4 = " ";
    this.SEG5 = " GROUP BY \n";
    this.SEG6 = " ";

    //input tables here and gen sql
    this.getSql = function(tables){
      var object = {
        success:false,
        message:""
      }
      var factTableCount = 0;
      if(angular.isArray(tables)){
        for(var i=0;i<tables.length;i++){
          var table = tables[i];
          if(table.isFactTable){
            _this.factTable = table;
            factTableCount++;
          }else{
            _this.lookupTables.push(table);
          }
        }

        if(factTableCount==0){
          object.message = "No fact table";
          return object;
        }
        else if(factTableCount>1){
          object.message = "More than one fact table defined.";
          return object;
        }

        _this.SEG3 = _this.getSeg3(_this.fact);
        _this.SEG4 = _this.getSeg4(_this.factTable,_this.lookupTables);
        _this.SEG6 = _this.getSeg6(_this.factTable,_this.lookupTables);


      }

    }

    this.getSeg1 = function(){
      return _this.SEG1;

    }

    //TO-DO get seg2
    this.getSeg2 = function(fact){
      var factFullName = fact.name;
      if(fact.table_SCHEM){
        factFullName = fact.table_SCHEM+"."+factFullName;
      }
      var measureSeg = " ";
      var measures = _this.tableMeasures[factFullName];
        for(var i=0;i<measures.length;i++){
          var meaSeg = " ";
          var measure = measures[i];
          if(measure.mea_type=="column"){
            if(measure.expression!="COUNT_DISTINCT"){

            }

          }
        }

    }
    this.getSeg3 = function(fact){
      var sqlSeg = " FROM ";
      var factFullName = fact.name;
      if(fact.table_SCHEM){
        factFullName = fact.table_SCHEM+"."+factFullName;
      }
      sqlSeg += " "+factFullName+" \n";
    }

    this.getSeg4 = function(fact, lookups){
      var sqlSeg = " ";
      var factFullName = "",lookupFullName ="";

      for(var i=0;i<lookups.length;i++){
        var lookup = lookups[i];

        if(lookup.table_SCHEM && lookup.table_SCHEM == fact.table_SCHEM){
           factFullName = fact.table_SCHEM+"."+fact.name;
           lookupFullName = lookup.table_SCHEM+"."+lookup.name;
        }else{
           factFullName = fact.name;
           lookupFullName = lookup.name;
        }

         var joinEntity = _this.tableModelJoin[factFullName][lookupFullName];
         var joinType = joinEntity.type;
         var joinSeg = " "+joinType.toUpperCase()+" JOIN "+lookupFullName+" \n";

        sqlSeg+=joinSeg;

        var onSeg = " ON ";
        for(var i=0;i<joinEntity.foreign_key.length;i++){
          var fk = joinEntity.foreign_key[i];
          var pk = joinEntity.primary_key[i];
          if(i==0){
            onSeg = onSeg+" "+factFullName.fk +" = " + lookupFullName.pk+" \n";
          }else{
            onSeg = onSeg + " AND "+factFullName.fk +" = " + lookupFullName.pk+" \n";
          }

        }
        sqlSeg += onSeg;
      }

      return sqlSeg;

    }

    this.getSeg6 = function(fact,lookups){
      var sqlSeg = " ";
      var factFullName = "",lookupFullName ="";

      var lookupDimSeg = " ";
      for(var i=0;i<lookups.length;i++) {
        var lookup = lookups[i];

        if (lookup.table_SCHEM && lookup.table_SCHEM == fact.table_SCHEM) {
          factFullName = fact.table_SCHEM + "." + fact.name;
          lookupFullName = lookup.table_SCHEM + "." + lookup.name;
        } else {
          factFullName = fact.name;
          lookupFullName = lookup.name;
        }

        //when join exist,gen dimension
        if(_this.tableModelJoin[factFullName][lookupFullName] && _this.tableModelJoin[factFullName][lookupFullName].type){
          var dimensions = _this.tableDimensions[lookupFullName];
          for(var i=0;i<dimensions.length;i++){
            lookupDimseg = lookupDimSeg +", "+lookupFullName+"."+dimension[i];
          }
        }

      }

      var factDimensions = _this.tableDimensions[factFullName];
      var factDimseg = " ";
      for(var i=0;i<factDimensions.length;i++){
        factDimseg = factDimseg + ", "+ factFullName+"."+factDimensions[i];
      }
      sqlSeg = factDimseg + lookupDimSeg;
      return sqlSeg;
    }

});
