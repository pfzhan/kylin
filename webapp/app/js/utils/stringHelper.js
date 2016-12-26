/**
 * Created by luguosheng on 16/12/8.
 */
'use strict';
/* String helper*/
KylinApp.factory('StringHelper', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
    removeNameSpace:function(str){
      if(str){
        return str.replace(/([^.\s]+\.)+/,'');
      }else{
        return '';
      }
    },
    cutStr:function(str,len,replaceStr){
      if(str){
        if(str.length>len){
          str=str.substr(0,len)+replaceStr;
        }
      }
      return str;
    }
  }
});
