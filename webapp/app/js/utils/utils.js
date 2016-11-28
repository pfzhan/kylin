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

'use strict';

/* utils */

KylinApp.factory('VdmUtil', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
    createDialog: function (template, scope, thenFunc, options) {
      options = (!!!options) ? {} : options;
      options = angular.extend({
        backdropFade: true,
        templateUrl: template,
        resolve: {
          scope: function () {
            return scope;
          }
        },
        controller: function ($scope, $modalInstance, scope) {
          $scope = angular.extend($scope, scope);
          $scope.animate = "fadeInRight";
          $scope.close = function (data) {
            $scope.animate = "fadeOutRight";
            $timeout(function () {
              $modalInstance.close(data);
            }, 500);
          }
        }
      }, options);

      var dialog = $modal.open(options);
      dialog.result.then(thenFunc);
    },

    formatDate: function (date, fmt) {
      var o = {
        "M+": date.getMonth() + 1,
        "d+": date.getDate(),
        "h+": date.getHours(),
        "m+": date.getMinutes(),
        "s+": date.getSeconds(),
        "q+": Math.floor((date.getMonth() + 3) / 3),
        "S": date.getMilliseconds()
      };
      if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
      for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));

      return fmt;
    },
    storage:{
      set :function(key,value){
        if($window.localStorage){
          $window.localStorage[key]=value;
        }
      },
      get:function(key,defaultValue){
        return  $window.localStorage&&$window.localStorage[key] || defaultValue;
      },
      setObject:function(key,value){
        if($window.localStorage){
          $window.localStorage[key]=JSON.stringify(value);
        }
      },
      getObject: function (key) {
        return JSON.parse($window.localStorage&&$window.localStorage[key] || '{}');
      },
      remove:function(key){
        if($window.localStorage){
          $window.localStorage.removeItem(key);
        }
      }
    },

    SCToFloat:function(data) {
      var resultValue = "";
      if (data&&data.indexOf('E') != -1) {
        var regExp = new RegExp('^((\\d+.?\\d+)[Ee]{1}(\\d+))$', 'ig');
        var result = regExp.exec(data);
        var power = "";
        if (result != null) {
          resultValue = result[2];
          power = result[3];
        }
        if (resultValue != "") {
          if (power != "") {
            var powVer = Math.pow(10, power);
            resultValue = (resultValue * powVer).toFixed(2);
          }
        }
      }
      return resultValue;
    },
    linkArrObjectToString:function(obj){
      var str='';
      for(var i in obj){
        if(/\d+/.test(i)){
          str+=obj[i];
        }
      }
      return str;
    },
    //过滤对象中的空值
    filterNullValInObj:function(needFilterObj){
      var newFilterObj,newObj;
      if(typeof needFilterObj=='string'){
        newObj=angular.fromJson(needFilterObj);
      }else{
        newObj=angular.extend({},needFilterObj);
      }
      function filterData(data){
        var obj=data;
        for(var i in obj){
          if(obj[i]===null){
            if(Object.prototype.toString.call(obj)=='[object Object]'){
              delete obj[i];
            }
          }
          else if(typeof obj[i]=== 'object'){
            obj[i]=filterData(obj[i]);
          }
        }
        return obj;
      }
      //if(typeof needFilterObj=='string'){
        return angular.toJson(filterData(newObj),true);
      //}else{
      //  return filterData(newObj)
      //}

    },getFilterObjectListByAndFilterVal:function(objList,key,value,matchkey,matchval){
      var len=objList&&objList.length|| 0,newArr=[];
      for(var i=0;i<len;i++){
        if(!key||value===objList[i][key]||(angular.isArray(value)&&value.indexOf(objList[i][key])>-1)){
          if(matchkey){
            if(matchval==objList[i][matchkey]||(angular.isArray(matchval)&&value.indexOf(objList[i][matchkey])>-1)){
              newArr.push(objList[i])
            }
          }else{
            newArr.push(objList[i])
          }
        }
      }
      return newArr;
    },
    getFilterObjectListByOrFilterVal:function(objList,key,val,orKey,orVal){
      var len=objList&&objList.length|| 0,newArr=[];
      for(var i=0;i<len;i++){
        if((key&&val===objList[i][key])||(orKey&&objList[i][orKey]===orVal)){
          newArr.push(objList[i]);
        }
      }
      return newArr;
    },
    removeFilterObjectList:function(objList,key,val){
      var len=objList&&objList.length|| 0,newArr=[];
      for(var i=0;i<len;i++){
        if(key&&val!=objList[i][key]){
          newArr.push(objList[i]);
        }
      }
      return newArr;
    },
    changeDataAxis:function(data){
      var len=data&&data.length|| 0,newArr=[];
      var sublen=data&&data.length&&data[0].length||0;
        for(var i=0;i<sublen;i++){
          var subArr=[];
          for(var j=0;j<len;j++){
            subArr.push(data[j][i]);
          }
          newArr.push(subArr);
        }
      return newArr;
    },
    removeNameSpace:function(str){
      if(str){
         return str.replace(/([^.\s]+\.)+/,'');
      }else{
        return '';
      }

    }
  }
});
