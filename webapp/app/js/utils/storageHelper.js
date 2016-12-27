/**
 * Created by luguosheng on 16/12/27.
 */
KylinApp.factory('StorageHelper', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
    storage:{
      set:function(key,value){
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
    }
  }
});
