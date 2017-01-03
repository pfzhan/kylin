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

KylinApp.factory('TableService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'tables/:tableName/:action/:database', {}, {
    list: {method: 'GET', params: {}, cache: true, isArray: true},
    get: {method: 'GET', params: {}, isArray: false},
    loadHiveTable: {method: 'POST', params: {}, isArray: false},
    unLoadHiveTable: {method: 'DELETE', params: {}, isArray: false},
    addStreamingSrc: {method: 'POST', params: {action:'addStreamingSrc'}, isArray: false},
    genCardinality: {method: 'PUT', params: {action: 'cardinality'}, isArray: false},
    showHiveDatabases: {method: 'GET', params: {action:'hive'}, cache: true, isArray: true},
    showHiveTables: {method: 'GET', params: {action:'hive'}, cache: true, isArray: true}
  });
}]);
KylinApp.factory('TableExtService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'table_ext/:projectName/:tableName/:action', {}, {
    getSampleInfo: {method: 'GET', params: {}, isArray: false},
    doSample: {method: 'PUT', params: {}, isArray: true},
    getCalcSampleProgress:{method:'GET',params: {}, isArray: false},
    loadHiveTable: {method: 'POST', params: {}, isArray: false},
    unLoadHiveTable: {method: 'DELETE', params: {}, isArray: false}
  });
}]);
