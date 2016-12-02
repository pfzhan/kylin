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

KylinApp.service('CubeDescModel', function (kylinConfig, modelsManager, TableModel) {

  this.cubeMetaFrame = {};

  //
  this.createNew = function () {
    var cubeMeta = {
      "name": "",
      "model_name": "",
      "description": "",
      "dimensions": [],
      "measures": [
        {
          "name": "_COUNT_",
          "function": {
            "expression": "COUNT",
            "returntype": "bigint",
            "parameter": {
              "type": "constant",
              "value": "1",
              "next_parameter":null
            },
            "configuration":{}
          }
        }
      ],
      "rowkey": {
        "rowkey_columns": []
      },
      "aggregation_groups": []
      ,
      "dictionaries" :[],
      "partition_date_start":0,
      "partition_date_end":undefined,
      "notify_list": [],
      "hbase_mapping": {
        "column_family": []
      },
      "status_need_notify":['ERROR', 'DISCARDED', 'SUCCEED'],
      "retention_range": "0",
      "auto_merge_time_ranges": [604800000, 2419200000],
      "engine_type": kylinConfig.getCubeEng(),
      "storage_type":kylinConfig.getStorageEng(),
      "override_kylin_properties":{}
    };

    return cubeMeta;
  };

  this.createMeasure = function () {
    var measure = {
      "name": "",
      "function": {
        "expression": "",
        "returntype": "",
        "parameter": {
          "type": "",
          "value": "",
          "next_parameter":null
        },
         "configuration":{}
      }
    };

    return measure;
  }

  this.createAggGroup = function () {
    var group = {
      "includes" : [],
      "select_rule" : {
        "hierarchy_dims" : [],//2 dim array
          "mandatory_dims" : [],
          "joint_dims" : [  ]//2 dim array
      }
    }

    return group;
  }

  this.createDictionaries = function () {
    var dictionaries = {
      "column": null,
      "builder": null,
      "reuse":null
    }
    return dictionaries;
  }

  this.initMeasures = function(arr,modelName,factTable){
    var model=modelsManager.getModel(modelName);
    var numbertype=["int","integer","smallint","bigint","tinyint","float","double","long"];
    angular.forEach(model.metrics,function(metric){
      if(!arr.filter(function(element,pos){return element.name==metric}).length){
        if((numbertype.indexOf(TableModel.tableColumnMap[factTable][factTable+"."+metric].datatype)!=-1)||(TableModel.tableColumnMap[factTable][factTable+"."+metric].datatype.substring(0,7)==="decimal")){
          arr.push(
            {"name": metric,
             "function": {
             "expression": "SUM" ,
             "parameter": {
               "type": "column",
               "value": metric,
               "next_parameter": null
             },
             "returntype": "decimal"
             },
           });
        }else{
          arr.push(
            {"name": metric,
             "function": {
               "expression": "COUNT_DISTINCT",
               "parameter": {
                 "type": "column",
                 "value": metric,
                 "next_parameter": null
               },
               "returntype": "hllc(10)"
             }
           });
        }
      }
    });
  }


})
