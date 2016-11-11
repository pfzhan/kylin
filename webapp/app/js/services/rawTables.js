/**
 * Created by luguosheng on 16/9/27.
 */
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

KylinApp.factory('RawTablesService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'rawtables/:rawTableName/:action', {}, {
    //list: {method: 'GET', params: {}, isArray: true},
    //getValidEncodings: {method: 'GET', params: {action:"validEncodings"}, isArray: true},
    //getCube: {method: 'GET', params: {}, isArray: false},
    //getSql: {method: 'GET', params: {propName: 'segs', action: 'sql'}, isArray: false},
    //updateNotifyList: {method: 'PUT', params: {propName: 'notify_list'}, isArray: false},
    //cost: {method: 'PUT', params: {action: 'cost'}, isArray: false},
    //rebuildLookUp: {method: 'PUT', params: {propName: 'segs', action: 'refresh_lookup'}, isArray: false},
    //rebuildCube: {method: 'PUT', params: {action: 'rebuild'}, isArray: false},
    //disable: {method: 'PUT', params: {action: 'disable'}, isArray: false},
    //enable: {method: 'PUT', params: {action: 'enable'}, isArray: false},
    //purge: {method: 'PUT', params: {action: 'purge'}, isArray: false},
    //clone: {method: 'PUT', params: {action: 'clone'}, isArray: false},
    //drop: {method: 'DELETE', params: {}, isArray: false},
    save: {method: 'POST', params: {}, isArray: false},
    update: {method: 'PUT', params: {}, isArray: false},
    delete:{method:'DELETE',params:{},isArray:false},
    enable:{method:'PUT',params:{action:'enable'},isArray:false},
    disable:{method:'PUT',params:{action:'disable'},isArray:false},
    getRawTableInfo: {method: 'GET', params:{},isArray:false}
  });
}]);
