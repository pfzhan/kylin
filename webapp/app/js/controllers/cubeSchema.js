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

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService,modelsManager, ProjectService, AuthenticationService,$filter,ModelService,MetaModel,CubeDescModel,CubeList,TableModel,ProjectModel,ModelDescService,SweetAlert,cubesManager,StreamingService,CubeService,language,VdmUtil) {

  $scope.language = language;
  $scope.modelsManager = modelsManager;
  $scope.cubesManager = cubesManager;
  $scope.projects = [];
  $scope.newDimension = null;
  $scope.newMeasure = null;
  $scope.forms={};

  $scope.dataKylin =  language.getDataKylin();
  $scope.wizardSteps = [
    {title: $scope.dataKylin.cube.schema[0], src: 'partials/cubeDesigner/info.html', isComplete: false,form:'cube_info_form'},
    {title: $scope.dataKylin.cube.schema[1], src: 'partials/cubeDesigner/dimensions.html', isComplete: false,form:'cube_dimension_form'},
    {title: $scope.dataKylin.cube.schema[2], src: 'partials/cubeDesigner/measures.html', isComplete: false,form:'cube_measure_form'},
    {title: $scope.dataKylin.cube.schema[3], src: 'partials/cubeDesigner/refresh_settings.html', isComplete: false,form:'refresh_setting_form'},
    {title: $scope.dataKylin.cube.schema[4], src: 'partials/cubeDesigner/advanced_settings.html', isComplete: false,form:'cube_setting_form'},
    {title: $scope.dataKylin.cube.schema[5], src: 'partials/cubeDesigner/rawtable_setting.html', isComplete: false,form:'rawtable_setting_form'},
    {title: $scope.dataKylin.cube.schema[6], src: 'partials/cubeDesigner/cubeOverwriteProp.html', isComplete: false,form:'cube_overwrite_prop_form'},
    {title: $scope.dataKylin.cube.schema[7], src: 'partials/cubeDesigner/overview.html', isComplete: false,form:null}
    ];

  $scope.curStep = $scope.wizardSteps[0];
  $scope.$on("finish",function (event,data) {
    $scope.dataKylin =  language.getDataKylin();
    for(var i in $scope.wizardSteps){
      $scope.wizardSteps[i].title = $scope.dataKylin.cube.schema[i];
    }

  });
  $scope.getTypeVersion=function(typename){
    var searchResult=/\[v(\d+)\]/.exec(typename);
    if(searchResult&&searchResult.length){
      return searchResult.length&&searchResult[1]||1;
    }else{
      return 1;
    }
  }
  $scope.removeVersion=function(typename){
    if(typename){
      return typename.replace(/\[v\d+\]/g,"").replace(/\s+/g,'');
    }
    return "";
  }

  //init encoding list
  $scope.store = {
    supportedEncoding:[],
    encodingMaps:{}
  }
  TableModel.getColumnTypeEncodingMap().then(function(data){
    $scope.store.encodingMaps=data;
  });
  CubeService.getValidEncodings({}, function (encodings) {
    if(encodings){
      for(var i in encodings)
        if(VdmUtil.isNotExtraKey(encodings,i)){
          var value = i
          var name = value;
          var typeVersion=+encodings[i]||1;
          var suggest=false,selecttips='';
          if(/\d+/.test(""+typeVersion)&&typeVersion>=1){
            for(var s=1;s<=typeVersion;s++){
              if(s==typeVersion){
                suggest=true;
              }
              if(value=="int"){
                name = "int (deprecated)";
                suggest=false;
              }
              if(typeVersion>1){
                selecttips=" (v"+s;
                if(s==typeVersion){
                  selecttips+=",suggest"
                }
                selecttips+=')';
              }
              $scope.store.supportedEncoding.push({
                "name":name+selecttips,
                "value":value+"[v"+s+"]",
                "version":typeVersion,
                "baseValue":value,
                "suggest":suggest
              });
            }
          }
        }
    }
  },function(e){
    $scope.store.supportedEncoding = $scope.cubeConfig.encodings;
  })
  $scope.getEncodings =function (name){
    var filterName=name;
    var columnType= $scope.modelsManager.getColumnTypeByColumnName(filterName);
    var matchList=VdmUtil.getObjValFromLikeKey($scope.store.encodingMaps,columnType);
    var encodings =$scope.store.supportedEncoding,filterEncoding;
    if($scope.isEdit){
      var rowkey_columns=$scope.cubeMetaFrame.rowkey.rowkey_columns;
      if(rowkey_columns&&filterName){
        for(var s=0;s<rowkey_columns.length;s++){
          if(filterName==rowkey_columns[s].column){
            var version=rowkey_columns[s].encoding_version;
            var noLenEncoding=rowkey_columns[s].encoding.replace(/:\d+/,"");
            filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'value',noLenEncoding+(version?"[v"+version+"]":"[v1]"),'suggest',true)
            matchList.push(noLenEncoding);
            filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList);
            break;
          }
        }
      }else{
        filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
        filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList)
      }
    }else{
      filterEncoding=VdmUtil.getFilterObjectListByOrFilterVal(encodings,'suggest',true);
      filterEncoding=VdmUtil.getObjectList(filterEncoding,'baseValue',matchList)
    }
    return filterEncoding;
  }
  $scope.allCubes = [];

  // ~ init
  if (!$scope.state) {
    $scope.state = {mode: "view"};
  }


  $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
    if(!newValue){
      return;
    }
    if ($scope.cubeMode=="editExistCube"&&newValue && !newValue.project) {
      initProject();
    }

  });

  // ~ public methods
  $scope.filterProj = function(project){
    return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project,$scope.permissions.ADMINISTRATION.mask);
  };


  $scope.removeElement = function (arr, element) {
    var index = arr.indexOf(element);
    if (index > -1) {
      arr.splice(index, 1);
    }
  };

  $scope.open = function ($event) {
    $event.preventDefault();
    $event.stopPropagation();

    $scope.opened = true;
  };

  $scope.preView = function () {
    var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
    if (stepIndex >= 1) {
      $scope.curStep.isComplete = false;
      $scope.curStep = $scope.wizardSteps[stepIndex - 1];
    }
  };

  $scope.nextView = function () {
    var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);

    if (stepIndex < ($scope.wizardSteps.length - 1)) {
      $scope.curStep.isComplete = true;
      $scope.curStep = $scope.wizardSteps[stepIndex + 1];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  };

  $scope.goToStep = function(stepIndex){
    if($scope.cubeMode == "addNewCube"){
      /*if(stepIndex+1>=$scope.curStep.step){
        return;
      }*/
    }
    for(var i=0;i<$scope.wizardSteps.length;i++){
      if(i<=stepIndex){
        $scope.wizardSteps[i].isComplete = true;
      }else{
        $scope.wizardSteps[i].isComplete = false;
      }
    }
    if (stepIndex < ($scope.wizardSteps.length)) {
      $scope.curStep = $scope.wizardSteps[stepIndex];

      AuthenticationService.ping(function (data) {
        UserService.setCurUser(data);
      });
    }
  }

  $scope.$watch('cube.detail', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if (newValue && $scope.state.mode === "view") {
      $scope.cubeMetaFrame = newValue;

      // when viw state,each cubeSchema has its own metaModel
      $scope.metaModel = {
        model: {}
      }
      $scope.metaModel.model=modelsManager.getModel($scope.cubeMetaFrame.model_name);
      $scope.metaModel.model.aliasColumnMap=angular.copy(MetaModel.setMetaModel($scope.metaModel.model).aliasColumnMap);
    }
  });
  $scope.$watch('cubeMetaFrame.model_name', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    $scope.metaModel.model = modelsManager.getModel(newValue);
    if($scope.metaModel.model){
      $scope.metaModel.model.aliasColumnMap=angular.copy(MetaModel.setMetaModel($scope.metaModel.model).aliasColumnMap);
      $scope.modelsManager.initAliasMapByModelSchema($scope.metaModel);
    }
    if(!$scope.metaModel.model){
      return;
    }
  });
  $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
    if (!newValue) {
      return;
    }
    if ($scope.cubeMode == "editExistCube" && newValue && !newValue.project) {
      initProject();
    }

  });

  $scope.checkCubeForm = function(stepIndex){
    // do not check for Prev Step
    if (stepIndex + 1 < $scope.curStep.step) {
      return true;
    }

    if(!$scope.curStep.form){
      return true;
    }
    if($scope.state.mode==='view'){
      return true;
    }
    else{
      //form validation
      if($scope.forms[$scope.curStep.form].$invalid){
        $scope.forms[$scope.curStep.form].$submitted = true;
        return false;
      }else{
        //business rule check
        switch($scope.curStep.form){
          case 'cube_info_form':
            return $scope.check_cube_info();
            break;
          case 'cube_dimension_form':
            return $scope.check_cube_dimension();
            break;
          case 'cube_measure_form':
            return $scope.check_cube_measure();
            break;
          case 'cube_setting_form':
            return $scope.check_cube_setting();
          case 'cube_overwrite_prop_form':
            return $scope.cube_overwrite_prop_check();
          case 'rawtable_setting_form':
            return $scope.rawtable_data_right_check();
          default:
            return true;
            break;
        }
      }
    }
  };

  $scope.check_cube_info = function(){
    var queryParam = {offset: 0, limit: 65535};
    CubeService.list(queryParam, function (all_cubes) {
      if($scope.allCubes.length > 0){
        $scope.allCubes.splice(0,$scope.allCubes.length);
      }

      for (var i = 0; i < all_cubes.length; i++) {
        $scope.allCubes.push(all_cubes[i].name.toUpperCase());
      }
    });

    if(($scope.state.mode === "edit") &&$scope.cubeMode=="addNewCube"&&($scope.allCubes.indexOf($scope.cubeMetaFrame.name.toUpperCase()) >= 0)){
      SweetAlert.swal($scope.dataKylin.alert.oops, $scope.dataKylin.alert.warning_cube_part_one + $scope.cubeMetaFrame.name.toUpperCase() + $scope.dataKylin.alert.warning_cube_part_two, 'warning');
      return false;
    }
    return true;
  }

    $scope.check_cube_dimension = function(){
        var errors = [];
        if(!$scope.cubeMetaFrame.dimensions.length){
            errors.push($scope.dataKylin.alert.check_cube_dimension);
        }
        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    };

    $scope.check_cube_measure = function(){
        var _measures = $scope.cubeMetaFrame.measures;
        var errors = [];
        if(!_measures||!_measures.length){
            errors.push($scope.dataKylin.alert.check_cube_measure_define);
        }

        var existCountExpression = false;
        for(var i=0;i<_measures.length;i++){
            if(_measures[i].function.expression=="COUNT"){
                existCountExpression=true;
                break;
            }
        }
        if(!existCountExpression){
            errors.push($scope.dataKylin.alert.check_cube_measure_metric);
        }

        var errorInfo = "";
        angular.forEach(errors,function(item){
            errorInfo+="\n"+item;
        });
        if(errors.length){
            SweetAlert.swal('', errorInfo, 'warning');
            return false;
        }else{
            return true;
        }
    }

    $scope.check_cube_setting = function(){
        var errors = [];

        angular.forEach($scope.cubeMetaFrame.aggregation_groups,function(group){
            if(!group||!group.includes||group.includes.length==0){
                errors.push($scope.dataKylin.alert.check_cube_aggregation_groups);
            }
        })

      var shardRowkeyList = [];
      angular.forEach($scope.cubeMetaFrame.rowkey.rowkey_columns,function(rowkey){
        if(rowkey.isShardBy == true){
          shardRowkeyList.push(rowkey.column);
        }
        if(rowkey.encoding.substr(0,3)=='int' && (rowkey.encoding.substr(4)<1 || rowkey.encoding.substr(4)>8)){
          errors.push($scope.dataKylin.alert.check_cube_rowkeys_int);
        }
      })
      if(shardRowkeyList.length >1){
          errors.push($scope.dataKylin.alert.check_cube_rowkeys_shard);
      }
      var cfMeasures = [];
      angular.forEach($scope.cubeMetaFrame.hbase_mapping.column_family,function(cf){
        angular.forEach(cf.columns[0].measure_refs, function (measure, index) {
          cfMeasures.push(measure);
        });
      });
      var uniqCfMeasures = _.uniq(cfMeasures);
      if(uniqCfMeasures.length != $scope.cubeMetaFrame.measures.length) {
        errors.push($scope.dataKylin.alert.check_cube_column_family);
      }
      var isCFEmpty = _.some($scope.cubeMetaFrame.hbase_mapping.column_family, function(colFamily) {
        return colFamily.columns[0].measure_refs.length == 0;
      });
      if (isCFEmpty == true) {
        errors.push($scope.dataKylin.alert.check_cube_column_family_empty);
      }
      var errorInfo = "";
      angular.forEach(errors,function(item){
          errorInfo+="\n"+item;
      });
      if(errors.length){
        SweetAlert.swal('', errorInfo, 'warning');
        return false;
      }else{
        return true;
      }
    }

  $scope.cube_overwrite_prop_check = function(){
    var errors = [];

    for(var key in $scope.cubeMetaFrame.override_kylin_properties){
      if(key==''){
        errors.push($scope.dataKylin.cube.tip_cubeCOKey);
      }
      if($scope.cubeMetaFrame.override_kylin_properties[key] == ''){
        errors.push($scope.dataKylin.cube.tip_cubeCOValue);
      }
    }

    var errorInfo = "";
    angular.forEach(errors,function(item){
      errorInfo+="\n"+item;
    });
    if(errors.length){
      SweetAlert.swal('', errorInfo, 'warning');
      return false;
    }else{
      return true;
    }
  }

   $scope.rawtable_data_right_check=function(){
     if(!$scope.RawTables||!$scope.RawTables.columns||$scope.RawTables.columns&&$scope.RawTables.columns.length<=0){
        return true;
     }
     var sortedCount= 0,setTypeError=false;
     for(var i=0;i<$scope.RawTables.columns.length;i++){
       if($scope.RawTables.columns[i].index=='sorted'){
         if(['date','time', 'integer'].indexOf($scope.RawTables.columns[i].encodingName)<0){
           setTypeError=true;
         }
         sortedCount++;
       }
     }
     var errorInfo;
     if(setTypeError){
       errorInfo=$scope.dataKylin.cube.rawtableMustSetSortedWidthDateEncoding;
     }
     else if(sortedCount==0){
       errorInfo=$scope.dataKylin.cube.rawtableMustSetSorted;
     }else if(sortedCount==1){
       return true;
     }else{
       errorInfo=$scope.dataKylin.cube.rawtableMustSetSingleSorted;
     }
     SweetAlert.swal('', errorInfo, 'warning');
     return false;
   }
  // ~ private methods
  function initProject() {
    ProjectService.listReadable({}, function (projects) {
      $scope.projects = projects;

      var cubeName = (!!$scope.routeParams.cubeName)? $scope.routeParams.cubeName:$scope.state.cubeName;
      if (cubeName) {
        var projName = null;
        if(ProjectModel.getSelectedProject()){
          projName=ProjectModel.getSelectedProject();
        }else{
          angular.forEach($scope.projects, function (project, index) {
            angular.forEach(project.realizations, function (unit, index) {
              if (!projName && unit.type=="CUBE"&&unit.realization === cubeName) {
                projName = project.name;
              }
            });
          });
        }

        if(!ProjectModel.getSelectedProject()){
          ProjectModel.setSelectedProject(projName);
          TableModel.aceSrcTbLoaded();
        }

        $scope.cubeMetaFrame.project = projName;
      }

      angular.forEach($scope.projects, function (project, index) {
        $scope.listAccess(project, 'ProjectInstance');
      });
    });
  }
});
