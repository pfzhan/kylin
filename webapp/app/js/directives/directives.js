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

/* Directives */

KylinApp.directive('kylinPagination', function ($parse, $q, language) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'partials/directives/pagination.html',
    link: function (scope, element, attrs) {
      var _this = this;
      scope.limit = 15;
      scope.hasMore = false;
      scope.language = language;
      scope.data = $parse(attrs.data)(scope.$parent);
      scope.action = $parse(attrs.action)(scope.$parent);
      scope.loadFunc = $parse(attrs.loadFunc)(scope.$parent);
      scope.autoLoad = true;


      scope.$watch("action.reload", function (newValue, oldValue) {
        if (newValue != oldValue) {
          scope.reload();
        }
      });

      var autoLoad = $parse(attrs.autoLoad)(scope.$parent);
      if (autoLoad == false) {
        scope.autoLoad = autoLoad;
      }

      scope.getLength = function (object) {
        if (!object) {
          return 0;
        }
        if (Object.prototype.toString.call(object) === '[object Array]') {
          return object.length;
        }
        else {
          var size = 0, key;
          for (key in object) {
            if (object.hasOwnProperty(key) && key != 'reload') size++;
          }

          return size;
        }
      }

      scope.reload = function () {
        var length = scope.getLength(scope.data);
        scope.loadFunc(0, scope.limit).then(function (dataLength) {
          scope.data = $parse(attrs.data)(scope.$parent);
          scope.hasMore = dataLength == scope.limit;

          return scope.data;
        });
      }

      if (scope.autoLoad) {
        scope.reload();
      }

      scope.loaded = true;
      return scope.showMore = function () {
        var loadPromise,
          _this = this;
        scope.loaded = false;
        var promises = [];
        var length = scope.getLength(scope.data);
        loadPromise = scope.loadFunc(length, scope.limit).then(function (dataLength) {
          scope.data = $parse(attrs.data)(scope.$parent);
          scope.hasMore = (dataLength == scope.limit);

          return scope.data;
        });
        promises.push(loadPromise);

        return $q.all(promises).then(function () {
          return scope.loaded = true;
        });
      };
    }
  };
})
  .directive('loading', function ($parse, $q) {
    return {
      restrict: 'E',
      scope: {},
      templateUrl: 'partials/directives/loading.html',
      link: function (scope, element, attrs) {
        scope.text = (!!!attrs.text) ? 'Loading...' : attrs.text;
        scope.seq = attrs.seq;
      }
    };
  })
  .directive('noResult', function ($parse, $q,language) {
    return {
      scope: {
        text:'@text'
      },
      templateUrl: 'partials/directives/noResult.html',
      link: function (scope, element, attrs) {
        scope.$watch('text',function(newTitle,oldTitle) {
          scope.text = (!!!attrs.text) ? (language.getLanguageType()==0?'No Result.':'无相关结果') : attrs.text;
        })
      }
    };
  }).directive('showonhoverparent',
  function() {
    return {
      link : function(scope, element, attrs) {
        element.parent().bind('mouseenter', function(e) {
          e.stopPropagation();
          element.show();
        });
        element.parent().bind('mouseleave', function(e) {
          e.stopPropagation();
          element.hide();
        });
      }
    };
  })
  .directive('typeahead', function ($timeout, $filter) {
    return {
      restrict: 'AEC',
      scope: {
        items: '=',
        prompt: '@',
        title: '@',
        model: '=',
        required: '@'
      },
      templateUrl: 'partials/directives/typeahead.html',
      link: function (scope, elem, attrs) {
        scope.current = null;
        scope.selected = true; // hides the list initially

        scope.handleSelection = function () {
          scope.model = scope.current[scope.title];
          scope.current = null;
          scope.selected = true;
        };
        scope.isCurrent = function (item) {
          return scope.current == item;
        };
        scope.setCurrent = function (item) {
          scope.current = item;
        };
        scope.keyListener = function (event) {
          var list, idx;
          switch (event.keyCode) {
            case 13:
              scope.handleSelection();
              break;
            case 38:
              list = $filter('filter')(scope.items, {name: scope.model});
              scope.candidates = $filter('orderBy')(list, 'name');
              idx = scope.candidates.indexOf(scope.current);
              if (idx > 0) {
                scope.setCurrent(scope.candidates[idx - 1]);
              } else if (idx == 0) {
                scope.setCurrent(scope.candidates[scope.candidates.length - 1]);
              }
              break;
            case 40:
              list = $filter('filter')(scope.items, {name: scope.model});
              scope.candidates = $filter('orderBy')(list, 'name');
              idx = scope.candidates.indexOf(scope.current);
              if (idx < scope.candidates.length - 1) {
                scope.setCurrent(scope.candidates[idx + 1]);
              } else if (idx == scope.candidates.length - 1) {
                scope.setCurrent(scope.candidates[0]);
              }
              break;
            default:
              break;
          }
        };

      }
    };
  })
  .directive('autoFillSync', function ($timeout) {
    return {
      require: 'ngModel',
      link: function (scope, elem, attrs, ngModel) {
        var origVal = elem.val();
        $timeout(function () {
          var newVal = elem.val();
          if (ngModel.$pristine && origVal !== newVal) {
            ngModel.$setViewValue(newVal);
          }
        }, 500);
      }
    }
  }).directive('retentionFormat', function() {
    return {
      require: 'ngModel',
      link: function(scope, element, attrs, ngModelController) {
        ngModelController.$parsers.push(function(data) {
          //convert data from view format to model format
          return data*86400000; //converted
        });

        ngModelController.$formatters.push(function(data) {
          //convert data from model format to view format
          return data/86400000; //converted
        });
      }
    }
  }).directive('datepickerTimezone', function () {
    // this directive workaround to convert GMT0 timestamp to GMT date for datetimepicker
    return {
      restrict: 'A',
      priority: 1,
      require: 'ngModel',
      link: function (scope, element, attrs, ctrl) {
        ctrl.$formatters.push(function (value) {

          //set null for 0
          if(value===0){
            return null;
          }

          //return value;
          var date = new Date(value + (60000 * new Date().getTimezoneOffset()));
          return date;
        });

        ctrl.$parsers.push(function (value) {
          if (isNaN(value)||value==null) {
            return value;
          }
          value = new Date(value.getFullYear(), value.getMonth(), value.getDate(), 0, 0, 0, 0);
          return value.getTime()-(60000 * value.getTimezoneOffset());
        });
      }
    };
  }).directive('dateTimepickerTimezone', function () {
  // this directive workaround to convert GMT0 timestamp to GMT date for datepicker
  return {
    restrict: 'A',
    priority: 1,
    require: 'ngModel',
    link: function (scope, element, attrs, ctrl) {
      ctrl.$formatters.push(function (value) {

        //set null for 0
        if(value===0){
          return '';
        }

        //return value;
        var newDate = new Date(value);

        var year = newDate.getUTCFullYear();
        var month = (newDate.getUTCMonth()+1)<10?'0'+(newDate.getUTCMonth()+1):(newDate.getUTCMonth()+1);
        var date = newDate.getUTCDate()<10?'0'+newDate.getUTCDate():newDate.getUTCDate();

        var hour = newDate.getUTCHours()<10?'0'+newDate.getUTCHours():newDate.getUTCHours();
        var mins = newDate.getUTCMinutes()<10?'0'+newDate.getUTCMinutes():newDate.getUTCMinutes();
        var seconds = newDate.getUTCSeconds()<10?'0'+newDate.getUTCSeconds():newDate.getUTCSeconds();

        var viewVal = year+"-"+month+"-"+date+" "+hour+":"+mins+":"+seconds;
        return viewVal;
      });

      ctrl.$parsers.push(function (value) {
        var date;
        if(/^\d{4}-\d{1,2}-\d{1,2}(\s+\d{1,2}:\d{1,2}:\d{1,2})?$/.test(value)) {
          date=new Date(value);
          if(!date||date&&!date.getTime()){
            return value;
          }else{
            var dateSplit=value.replace(/^\s+|\s+$/,'').replace(/\s+/,'-').split(/[:-]/);
            var resultDate=[];
            for(var i=0;i<6;i++){
              resultDate[i]=dateSplit[i]||0;
            }
            return Date.UTC(resultDate[0],resultDate[1]-1,resultDate[2],resultDate[3],resultDate[4],resultDate[5]);
          }
        }else{
          return value;
        }
      });
    }
  };
}).directive('repeatFinish',function(){
    return {
      link: function(scope,element,attr){
        if(scope.$last == true){
          scope.$eval( attr.repeatFinish )
        }
      }
    }
  })
  .directive("parametertree", function($compile) {
    return {
      restrict: "E",
      transclude: true,
      scope: {
        nextpara: '='
      },
      template:
      '<li class="parent_li">Value:<b>{{nextpara.value}}</b>, Type:<b>{{ nextpara.type }}</b></li>' +
       '<parametertree ng-if="nextpara.next_parameter!=null" nextpara="nextpara.next_parameter"></parameterTree>',
      compile: function(tElement, tAttr, transclude) {
        var contents = tElement.contents().remove();
        var compiledContents;
        return function(scope, iElement, iAttr) {
          if(!compiledContents) {
            compiledContents = $compile(contents, transclude);
          }
          compiledContents(scope, function(clone, scope) {
            iElement.append(clone);
          });
        };
      }
    };
  }).directive("groupbytree", function($compile) {
    return {
      restrict: "E",
      transclude: true,
      scope: {
        nextpara: '=',
      },
      template:
      '<b>{{nextpara.value}}<b ng-if="nextpara.next_parameter!=null">,</b></b>'+
      '<groupbytree ng-if="nextpara.next_parameter!=null" nextpara="nextpara.next_parameter"></groupbytree>',
      compile: function(tElement, tAttr, transclude) {
        var contents = tElement.contents().remove();
        var compiledContents;
        return function(scope, iElement, iAttr) {
          if(!compiledContents) {
            compiledContents = $compile(contents, transclude);
          }
          compiledContents(scope, function(clone, scope) {
            iElement.append(clone);
          });
        };
      }
    };
  }).directive("topntree", function($compile) {
  return {
    restrict: "E",
    transclude: true,
    scope: {
      nextpara: '='
    },
    template:
    '<li class="parent_li">SUM|ORDER BY:<b>{{nextpara.value}}</b></b></li>' +
    '<li class="parent_li">Group By:'+
    '<groupbytree nextpara="nextpara.next_parameter"></groupbytree>'+
    '</li>',
    compile: function(tElement, tAttr, transclude) {
      var contents = tElement.contents().remove();
      var compiledContents;
      return function(scope, iElement, iAttr) {
        if(!compiledContents) {
          compiledContents = $compile(contents, transclude);
        }
        compiledContents(scope, function(clone, scope) {
          iElement.append(clone);
        });
      };
    }
  };
}).directive('kylinpopover', function ($compile,$templateCache,language) {
  return {
    restrict: "EA",
    scope:{
        title:'@ngTitle',
        cube: '@ngCube',
        value:'@ngValue',
        name: '@ngName',
        version:'@ngVersion'
    },
    link: function (scope, element, attrs) {
      scope.$watch(function() {
        if(!!attrs.ngWatch){
        return attrs.ngTitle+''+attrs.ngWatch;
        }else{
          return attrs.ngTitle;
        }
      },function(newTitle,oldTitle){
        var popOverContent;
        scope.Math = window.Math;
        scope.dataKylin = language.getDataKylin();
        var dOptions = {
          placement : 'right'
        }
        popOverContent = $compile($templateCache.get(attrs.template))(scope);
        var placement = attrs.placement? attrs.placement : dOptions.placement;
        //var title = attrs.title;
        var options = {
          content: popOverContent,
          placement: placement,
          trigger: "hover",
          title: attrs.ngTitle,
          html: true
        };
        $(element).popover('destroy');
        if(options.title!=""&&options.content!=""){
          $(element).popover(options);
        }
      });

    }
  };
}).directive("extendedcolumntree", function($compile) {
  return {
    restrict: "E",
    transclude: true,
    scope: {
      nextpara: '='
    },
    template:
    '<li class="parent_li">Host Column:<b>{{nextpara.value}}</b></b></li>' +
    '<li class="parent_li">Extended Column:<b>{{nextpara.next_parameter.value}}</b></li>',
    compile: function(tElement, tAttr, transclude) {
      var contents = tElement.contents().remove();
      var compiledContents;
      return function(scope, iElement, iAttr) {
        if(!compiledContents) {
          compiledContents = $compile(contents, transclude);
        }
        compiledContents(scope, function(clone, scope) {
          iElement.append(clone);
        });
      };
    }
  };
}).directive('extendedColumnReturn', function() {
  return {
    require: 'ngModel',
    link: function(scope, element, attrs, ngModelController) {

      var prefix = "extendedcolumn(";
      var suffix = ")";
      ngModelController.$parsers.push(function(data) {
        //convert data from view format to model format
        return prefix +data+suffix; //converted
      });

      ngModelController.$formatters.push(function(data) {
        //convert data from model format to view format
        var prefixIndex = data.indexOf("(")+1;
        var suffixIndex = data.indexOf(")");
        return data.substring(prefixIndex,suffixIndex); //converted
      });
    }
  }
});
