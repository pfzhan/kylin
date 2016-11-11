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

KylinApp.service('GraphService', function (GraphBuilder,VdmUtil) {

  this.buildGraph = function (query) {
    var graphData = null;
    var dimension = query.graph.state.dimensions;

    if (dimension && query.graph.type.dimension.types.indexOf(dimension.type) > -1) {
      var metricsList = [];
      metricsList = metricsList.concat(query.graph.state.metrics);
      angular.forEach(metricsList, function (metrics, index) {
        var aggregatedData = {};
        angular.forEach(query.result.results,function(row,index){
          angular.forEach(row,function(column,value){
            var float = VdmUtil.SCToFloat(column);
              if (float!=""){
                query.result.results[index][value]=float;
              }
          });
        });
        angular.forEach(query.result.results, function (data, index) {
          aggregatedData[data[dimension.index]] = (!!aggregatedData[data[dimension.index]] ? aggregatedData[data[dimension.index]] : 0)
          + parseFloat(data[metrics.index].replace(/[^\d\.\-]/g, ""));
        });

        var newData = GraphBuilder["build" + capitaliseFirstLetter(query.graph.type.value) + "Graph"](dimension, metrics, aggregatedData);
        graphData = (!!graphData) ? graphData.concat(newData) : newData;
      });
    }

    return graphData;
  }

  function capitaliseFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
  }
});
