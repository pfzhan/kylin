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

KylinApp.factory('GraphBuilder', function () {
  var graphBuilder = {};

  graphBuilder.buildLineGraph = function (dimension, metrics, aggregatedData) {
    var values = [];
    angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
      values.push([(dimension.type == 'date') ? moment(sortedKey).unix() : sortedKey, aggregatedData[sortedKey]]);
    });

    var newGraph = [
      {
        "key": metrics.column.label,
        "values": values
      }
    ];

    return newGraph;
  }

  graphBuilder.buildBarGraph = function (dimension, metrics, aggregatedData) {
    var newGraph = [];
    angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
      newGraph.push({
        key: sortedKey,
        values: [
          [sortedKey, aggregatedData[sortedKey]]
        ]
      });
    });

    return newGraph;
  }

  graphBuilder.buildPieGraph = function (dimension, metrics, aggregatedData) {
    var newGraph = [];
    angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
      newGraph.push({
        key: sortedKey,
        y: aggregatedData[sortedKey]
      });
    });

    return newGraph;
  }

  function getSortedKeys(results) {
    var sortedKeys = [];
    for (var k in results) {
      if (results.hasOwnProperty(k)) {
        sortedKeys.push(k);
      }
    }
    sortedKeys.sort();

    return sortedKeys;
  }

  return graphBuilder;
});
