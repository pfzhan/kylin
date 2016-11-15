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

/* Filters */
KylinApp
  .filter('orderObjectBy', function () {
    return function (input, attribute, reverse) {
      if (!angular.isObject(input)) return input;

      var array = [];
      for (var objectKey in input) {
        array.push(input[objectKey]);
      }

      array.sort(function (a, b) {
        if (!attribute) {
          return 0;
        }
        var result = 0;
        var attriOfA = a, attriOfB = b;
        var temps = attribute.split(".");
        if (temps.length > 1) {
          angular.forEach(temps, function (temp, index) {
            attriOfA = attriOfA[temp];
            attriOfB = attriOfB[temp];
          });
        }
        else {
          attriOfA = a[attribute];
          attriOfB = b[attribute];
        }

        if (!attriOfA) {
          result = -1;
        }
        else if (!attriOfB) {
          result = 1;
        }
        else {
          if(!isNaN(attriOfA)||!isNaN(attriOfB)){
            result = attriOfA > attriOfB ? 1 : attriOfA < attriOfB ? -1 : 0;
          }else{
            result = attriOfA.toLowerCase() > attriOfB.toLowerCase() ? 1 : attriOfA.toLowerCase() < attriOfB.toLowerCase() ? -1 : 0;
          }
        }
        return reverse ? -result : result;
      });
      return array;
    }
  })

  .filter('reverse', function () {
    return function (items) {
      if (items) {
        return items.slice().reverse();
      } else {
        return items;
      }
    }
  })
  .filter('range', function () {
    return function (input, total) {
      total = parseInt(total);
      for (var i = 0; i < total; i++)
        input.push(i);
      return input;
    }
  })
  // Convert bytes into human readable format.
  .filter('bytes', function () {
    return function (bytes, precision) {
      if (bytes === 0) {
        return 'less than 1 MB';
      }
      if (isNaN(parseFloat(bytes)) || !isFinite(bytes)) {
        return '-';
      }

      if (typeof precision === 'undefined') {
        precision = 3;
      }

      var units = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB'],
        number = Math.floor(Math.log(bytes) / Math.log(1024));
      switch(number){
        case 0:
          precision = 0;
          break;
        case 1:
        case 2:
          precision = 3;
          break;
        default:
          precision = 5;
      }

      return Math.round((bytes / Math.pow(1024, Math.floor(number)))*Math.pow(10,precision))/Math.pow(10,precision) + ' ' + units[number];
    }
  }).filter('resizePieHeight', function () {
    return function (item) {
      if (item < 150) {
        return 1300;
      }
      return 1300;
    }
  }).filter('utcToConfigTimeZone', function ($filter, kylinConfig) {

    var gmttimezone;
    //convert GMT+0 time to specified Timezone
    return function (item, timezone, format) {

      // undefined and 0 is not necessary to show
      if (angular.isUndefined(item) || item === 0) {
        return "";
      }

      if (angular.isUndefined(timezone) || timezone === '') {
        timezone = kylinConfig.getTimeZone() == "" ? 'PST' : kylinConfig.getTimeZone();
      }
      if (angular.isUndefined(format) || format === '') {
        format = "yyyy-MM-dd HH:mm:ss";
      }

      //convert short name timezone to GMT
      switch (timezone) {
        //convert PST to GMT
        case "PST":
          gmttimezone = "GMT-8";
          break;
        default:
          gmttimezone = timezone;
      }

      var localOffset = new Date().getTimezoneOffset();
      var convertedMillis = item;
      if (gmttimezone.indexOf("GMT+") != -1) {
        var offset = gmttimezone.substr(4, 1);
        convertedMillis = new Date(item).getTime() + offset * 60 * 60000 + localOffset * 60000;
      }
      else if (gmttimezone.indexOf("GMT-") != -1) {
        var offset = gmttimezone.substr(4, 1);
        convertedMillis = new Date(item).getTime() - offset * 60 * 60000 + localOffset * 60000;
      }
      else {
        // return PST by default
        timezone = "PST";
        convertedMillis = new Date(item).getTime() - 8 * 60 * 60000 + localOffset * 60000;
      }
      return $filter('date')(convertedMillis, format) + " " + timezone;

    }
  }).filter('reverseToGMT0', function ($filter) {
    //backend store GMT+0 timezone ,by default front will show local,so convert to GMT+0 Date String format
    return function (item) {
      if (item || item == 0) {
        item += new Date().getTimezoneOffset() * 60000;
        return $filter('date')(item, "yyyy-MM-dd HH:mm:ss");
      }
    }
  }).filter('millisecondsToDay', function ($filter) {
    //backend store GMT+0 timezone ,by default front will show local,so convert to GMT+0 Date String format
    return function (item) {
        return item/86400000;
    }
  }).filter('timeRangeFormat', function ($filter) {
    //backend store GMT+0 timezone ,by default front will show local,so convert to GMT+0 Date String format
    return function (item) {
      var _day = Math.floor(item/86400000);
      var _hour = (item%86400000)/3600000;
      if(_day==0){
        return _hour +" (Hours)"
      }else{
        return _day +" (Days)";
      }
    }
  }).filter('nullValFilter', function($filter) {
      return function(input) {
        return input == null ? 'null' : input;
      };
});
