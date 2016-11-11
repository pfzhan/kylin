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

KylinApp.service('StreamingList', function (CubeService, $q, AccessService, StreamingService) {
  var _this = this;
  this.streamingConfigs = [];
  this.kafkaConfigs = [];


  this.list = function () {
    var defer = $q.defer();

    var streamingPromises = [];
    var kafkaPromises = [];


    kafkaPromises.push(StreamingService.getKfkConfig({}, function (kfkConfigs) {
      _this.kafkaConfigs = kfkConfigs;
    },function(){
      defer.reject("Failed to load models");
    }).$promise);

    streamingPromises.push(StreamingService.getConfig({}, function (streamings) {
      _this.streamingConfigs = streamings;
    },function(){
      defer.reject("Failed to load models");
    }).$promise);

    $q.all(streamingPromises,kafkaPromises).then(function(result,rs){
      defer.resolve("success");
    },function(){
      defer.resolve("failed");
    })
    return defer.promise;

  };

  this.checkCubeExist = function(cubeName){
    var result = {streaming:null,exist:false};
    for(var i=0;i<_this.streamingConfigs.length;i++){
      if(_this.streamingConfigs[i].cubeName == cubeName){
        result ={
          streaming:_this.streamingConfigs[i],
          exist:true
        }
        break;
      }
    }
    return result;
  }

  this.getKafkaConfig = function(kfkName){
      for(var i=0;i<_this.kafkaConfigs.length;i++) {
        if(_this.kafkaConfigs[i].name == kfkName){
          return _this.kafkaConfigs[i];
        }
      }
    }

  this.removeAll = function () {
    _this.streamingConfigs = [];
    _this.kafkaConfigs = [];
  };

});
