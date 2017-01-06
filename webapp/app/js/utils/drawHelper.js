//snow model design
KylinApp.service('DrawHelper', function ($modal, $timeout, $location, $anchorScroll, $window,TableModel,ProjectModel,StorageHelper) {
    var DrawHelper= {
    titleHeight:60,
    itemHeight:20,
    itemWidth:300,
    boxWidth:800,
    boxHeight:1000,
    baseTop:10,
    levelTopList:[10],
    containerId:'snowBox',
    container:null,
    tableCount:1,
    instance:null,
    lastLink:[],
    connects:{},
    partitionDate:{},
    partitionTime:{},
    zoom:1,
    zoomRate:0.1,
    delTable:null,
    changeConnectType:null,
    addColumnToPartitionDate:null,
    addColumnToPartitionTime:null,
    openJsonDialog:null,
    saveModel:null,
    instanceName:'',
    instanceDiscribe:'',
    filterStr:'',
    maxZoom:1.2,
    minZoom:0.43,
    useCache:true,
    usedColumnsMap:[],
    containerSize:{
      width:100000,
      height:100000
    },
    actionLockForView:false,
    actionLockForEdit:false,
    unDraggaleList:['.bar_action','.tipToolbar','.plusHandle','.minusHandle'],
    //view级别的锁定
    checkLock:function(){
      if(this.actionLockForView){
        return true;
      }
    },
    init:function(para){
      this.container=$('#'+this.containerId);
      $.extend(this,para);
      var that=this;
      this.container.css({width:this.containerSize.width+'px',height:this.containerSize.height+'px',left:(-this.containerSize.width+this.container.parent().width())/2+'px',top:(-this.containerSize.height+this.container.parent().height())/2+'px'})
      this.container.parent().css({'height':$(window).height()+'px','overflow':'hidden'});
      this.instance = jsPlumb.getInstance({
          DragOptions: { cursor: 'pointer', zIndex: 2000},
          PaintStyle: {width: 25, height: 21, strokeStyle: '#66a8fa' },
          EndpointHoverStyle: { fillStyle: "yellow" },
          HoverPaintStyle: { strokeStyle: "yellow" },
          EndpointStyle: { width: 20, height: 16, stroke: '#666' },
          Endpoint: "Rectangle",
          Anchors: ["TopCenter", "TopCenter"],
          Container: that.containerId,
          ConnectionOverlays: [
            [ "Arrow", { location: 1 } ],
            [ "Label", {location: 0.1,id: "label",cssClass: "aLabel"}]
          ]
      });
      //全局链接事件
      this.instance.bind("connection", function (info, originalEvent) {
        //排除锁定的情况下人为拖动
        if(that.checkLock()&&that.beginDrag){
          that.instance.detach(info.connection);
          return;
        }
        that.lastLink=[info.sourceEndpoint.getParameters().data,info.targetEndpoint.getParameters().data];
        //自己不能连自己
        if(info.sourceId==info.targetId){
          that.instance.detach(info.connection);
          that.showTips('linklimit');
          return;
        }
        //rootFact不能连别人
        var rootFactTable=that.tableList.getRootFact();
        if(that.lastLink[0]&&rootFactTable&&rootFactTable.guid==that.lastLink[0].guid){
          that.instance.detach(info.connection);
          that.showTips('rootlimit');
          return;
        }

        //不同类型的主外键关联预警
        if(that.lastLink[0].type!=that.lastLink[1].type){
          that.showTips('difftype')
        }

        that.connects[info.connection.id]=[that.lastLink[0].column,that.lastLink[1].column];

        if(that.beginDrag){
          that.beginDrag=false;
          var joinType=that.getConnectType(that.lastLink[0].guid,that.lastLink[1].guid)||'';
          if(!joinType){
            that.changeConnectType(info.connection);
          }else{
            info.connection.getOverlay("label").setLabel(joinType);
            that.connects[info.connection.id][2]=joinType;
          }
        }
        //that.plumbDataToKylinData();
        info.connection.unbind('click').bind('click',function(conn){
          if(that.checkLock()){
            return;
          }
          if(conn.id!='label'){
            that.changeConnectType(info.connection);
          }
        })
        that.storeCache();
      });
      var $panzoom=this.container.panzoom({
        cursor: "zoom",
        minScale: 0.25,
        increment: 0.1,
        duration: 100,
        ignoreChildrensEvents:true
      });
      this.showMapControl();
      this.showActionBtn();
      for(var i=0;i<this.unDraggaleList.length;i++){
        (function(i){
          that.container.parent().find(that.unDraggaleList[i]).mousemove(function(e){
            e.stopPropagation();
          })
        }(i))
      }

      return this;
    },
    projectName:'',
    kylinData:null,
    //通过connects数据绘制连接线
    renderLinks:function(){
      var that=this,count=0;
      var cloneLinkes= $.extend(true,{},that.connects);
      that.instance.batch(function() {
        for(var i in cloneLinkes){
            var point1Obj=cloneLinkes[i][0].split('.');
            var point2Obj=cloneLinkes[i][1].split('.');
            var linkType=cloneLinkes[i][2];
            var point1=that.tableList.getTable('guid',point1Obj[0]).alias+'.'+point1Obj[1];
            var point2=that.tableList.getTable('guid',point2Obj[0]).alias+'.'+point2Obj[1];
            delete that.connects[i];
            that.connect(point1,point2);
            that.instance.getAllConnections()[count].getOverlay("label").setLabel(linkType||'');
            that.connects[that.instance.getAllConnections()[count].id][2]=linkType;
            count++;
        }
      })
      this.storeCache();
    },
    renderPartitionSetting:function(){
      for(var i in this.partitionDate){
        $('#column_'+i+this.partitionDate[i].columnName).find('.snowDate').addClass('.active');
      }
      for(var i in this.partitionTime){
        $('#column_'+i+this.partitionTime[i].columnName).find('.snowTime').addClass('.active');
      }
    },
    storeCache:function(){
      if(!this.useCache){
        return;
      }
      var that=this;
      var storeObj={};
      storeObj.tableList={};
      storeObj.tableList.data=that.tableList.data;
      storeObj.connects=that.connects;
      storeObj.partitionDate=that.partitionDate;
      storeObj.partitionTime=that.partitionTime;
      StorageHelper.storage.set('snowModelJsDragData',angular.toJson(storeObj,true));
    },
    restoreCache:function(){
      this.renderTables(this.tableList.data);
      this.renderLinks();
      this.renderPartitionSetting();
    },
    removeCache:function(){
      StorageHelper.storage.remove('snowModelJsDragData');
    },
    //kylin数据转化未plumbjs效果
    kylinDataToJsPlumbData:function(){
       var data=this.kylinData,that=this;
       var cloneData= $.extend(true,{},data);
       var factTable=cloneData.fact_table;
       var looks=[];
       var tableBaseList=[];
       if(factTable){
         tableBaseList.push({
           table:factTable,
           kind:'ROOTFACT',
           alias:factTable.replace(/[^.]+\./g,'')
         })
       }
       tableBaseList=tableBaseList.concat(cloneData.lookups);
       var projectName=ProjectModel.getProjectByCubeModel(cloneData.name);
       this.projectName=projectName;

       TableModel.initTableData(loadTableFunc,projectName)
       function loadTableFunc(){
         //加载table基础信息到面板
         for(var i=0;i<tableBaseList.length;i++){
           var tableDetail=TableModel.getTableDatail(tableBaseList[i].table);
           if(tableDetail&&tableDetail.length>0){
             tableBaseList[i].columns= $.extend(true,[],tableDetail[0].columns);
             tableBaseList[i].pos=[],
             tableBaseList[i].guid=that.guid();
             tableBaseList[i].name=tableBaseList[i].table.replace(/[^.]+\./g,'');
             tableBaseList[i].table=tableBaseList[i].table;
             for(var m=0;m<tableBaseList[i].columns.length;m++){
               if(tableBaseList[i].alias+'.'+tableBaseList[i].columns[m].name==cloneData.partition_desc.partition_date_column){
                 that.partitionDate[tableBaseList[i].guid]={
                   columnName:tableBaseList[i].columns[m].name,
                   dateType:cloneData.partition_desc.partition_date_format
                 }
                 tableBaseList[i].columns[m].isDate=true;
               }
               if(tableBaseList[i].alias+'.'+tableBaseList[i].columns[m].name==cloneData.partition_desc.partition_time_column){
                 that.partitionTime[tableBaseList[i].guid]={
                  columnName:tableBaseList[i].columns[m].name,
                  dateType:cloneData.partition_desc.partition_time_format
                 }
                 tableBaseList[i].columns[m].isTime=true;
               }
                tableBaseList[i].columns[m].kind='disable';
                if(cloneData.metrics.indexOf(tableBaseList[i].alias+'.'+tableBaseList[i].columns[m].name)>=0){
                  tableBaseList[i].columns[m].kind='measure';
                }
               for(var d=0;d<cloneData.dimensions.length;d++){
                 if(tableBaseList[i].alias==cloneData.dimensions[d].table&&cloneData.dimensions[d].columns.indexOf(tableBaseList[i].columns[m].name)>=0){
                   tableBaseList[i].columns[m].kind='dimension';
                   break;
                 }
               }

             }
           }
           that.aliasList.push(tableBaseList[i].alias);

         }

         that.addTables(tableBaseList);
         //加载链接
         that.instance.batch(function() {
           var count=0;
           for(var i=0;i<tableBaseList.length;i++){
             if(tableBaseList[i].join){
               for(var f=0;f<tableBaseList[i].join.primary_key.length;f++){
                 that.connect(tableBaseList[i].join.primary_key[f],tableBaseList[i].join.foreign_key[f]);
                 that.instance.getAllConnections()[count].getOverlay("label").setLabel(tableBaseList[i].join.type||'');
                 count++;
               }
             }
           }
         }, true);


         setTimeout(function(){
           if(cloneData.uuid){
             that.calcPosition();
           }
         },1)
       }
    },
    //操作台数据转换为kylin接受数据
    plumbDataToKylinData:function(){
      var that=this;
       var kylinData={
         lookups:[],
         partition_desc:{},
         dimensions:[],
         metrics:[],
         filter_condition:this.filterStr,
         name:this.instanceName,
         description:this.instanceDiscribe
       };
      //采集rootfacttable信息
       var rootFactTable=this.tableList.getRootFact();
      if(rootFactTable){
        kylinData.fact_table=rootFactTable.table;
      }
      //采集jontable的信息
       var lookups={},linkTables={};
       if(this.isNullObj(this.connects)&&this.tableList.data.length>=1){
         linkTables[this.tableList.data[0].guid]=true;
       }else{
         for(var i in this.connects){
           var joinTableGuid=this.connects[i][0].split('.')[0];
           if(!lookups[joinTableGuid]){
             lookups[joinTableGuid]={foreigns:[],primarys:[], type:''}
           }
           lookups[joinTableGuid].primarys.push(this.connects[i][0]);
           lookups[joinTableGuid].foreigns.push(this.connects[i][1]);
           linkTables[this.connects[i][0].split('.')[0]]=true;
           linkTables[this.connects[i][1].split('.')[0]]=true;
           lookups[joinTableGuid].type=this.connects[i][2];
         }
       }

       for(var i in lookups){
          var tableObj={};
          var tableBase=this.tableList.getTable('guid', i);
          tableObj.table=tableBase.table;
          tableObj.kind=tableBase.kind;
          tableObj.alias=tableBase.alias;
          tableObj.join={type:lookups[i].type,primary_key:[],foreign_key:[]};
          for(var s=0;s<lookups[i].foreigns.length;s++){
            var pTableBase=this.tableList.getTable('guid', lookups[i].primarys[s].split('.')[0]);
            tableObj.join.primary_key.push(pTableBase.alias+'.'+lookups[i].primarys[s].split('.')[1]);
            var fTableBase=this.tableList.getTable('guid', lookups[i].foreigns[s].split('.')[0]);
            tableObj.join.foreign_key.push( fTableBase.alias+'.'+lookups[i].foreigns[s].split('.')[1]);
          }
          kylinData.lookups.push(tableObj);
       }
      //采集partition 信息
      if(!this.isNullObj(this.partitionDate)){
        for(var i in this.partitionDate){
          var pTableBase=this.tableList.getTable('guid', i);
          kylinData.partition_desc.partition_date_column=pTableBase.alias+'.'+this.partitionDate[i].columnName;
          kylinData.partition_desc.partition_date_start=null;
          kylinData.partition_desc.partition_type='APPEND';
          kylinData.partition_desc.partition_date_format=this.partitionDate[i].dateType;

        }
      }
      if(!this.isNullObj(this.partitionTime)){
        for(var i in this.partitionTime){
          var pTableBase=this.tableList.getTable('guid', i);
          kylinData.partition_desc.partition_time_column=pTableBase.alias+'.'+this.partitionTime[i].columnName;
          kylinData.partition_desc.partition_time_start=null;
          kylinData.partition_desc.partition_time_format=this.partitionTime[i].dateType;
        }
      }
      //采集dimensions和measure信息
      for(var i in linkTables){
        var tableBase=this.tableList.getTable('guid', i);
        var tableObj={columns:[]};
        tableObj.table=tableBase.alias;
        for(var m=0;m<tableBase.columns.length;m++){
           if('dimension'==tableBase.columns[m].kind){
             tableObj.columns.push(tableBase.columns[m].name)
           }else if('measure'==tableBase.columns[m].kind){
             kylinData.metrics.push(tableBase.alias+'.'+tableBase.columns[m].name);
           }
        }
        if(tableObj.columns.length>0){
          kylinData.dimensions.push(tableObj);
        }
      }
      ////console.log(kylinData);
      //if(that.useCache){
      //  StorageHelper.storage.set('snowModelJsDragData',angular.toJson(kylinData,true));
      //}
      return kylinData;
    },
    getConnectType:function(sourceGuid,targetGuid){
      for(var i in this.connects){
        if(this.connects[i][0].indexOf(sourceGuid+'.')>=0&&this.connects[i][1].indexOf(targetGuid+'.')>=0){
          return this.connects[i][2];
        }
      }
      return '';
    },
    updataConnectsType:function(sourceGuid,targetGuid,type){
      for(var i in this.connects){
        if(this.connects[i][0].indexOf(sourceGuid+'.')>=0&&this.connects[i][1].indexOf(targetGuid+'.')>=0){
          this.connects[i][2]=type;
          for(var s=0;s<this.instance.getAllConnections().length;s++){
            if(this.instance.getAllConnections()[s].id==i){
              this.instance.getAllConnections()[s].getOverlay("label").setLabel(type);
            }
          }
        }
      }
      this.storeCache();
    },

    getForeignKeyCount:function(guid){
      var count=0;
      for(var i in this.connects){
        if(this.connects[i][0].indexOf(guid+'.')>=0){
          count+=1;
        }
      }
      return count;
    },
    getTableLinksCount:function(guid){
      var count=0;
      for(var i in this.connects){
        if(this.connects[i][0].indexOf(guid+'.')>=0||this.connects[i][1].indexOf(guid+'.')>=0){
          count+=1;
        }
      }
      return count;
    },
    showActionBtn:function(){
      if(this.checkLock()){
        return;
      }
      var that=this;
      var barActionBtn=this.container.nextAll('.bar_action').find('span');
      barActionBtn.eq(0).click(function(){
        that.saveModel();
      })
      barActionBtn.eq(1).click(function(){
        that.openJsonDialog();
      })

    },
    showTips:function(type){
       var tipDom= this.container.parent().find('.'+type);
       if(tipDom){
         tipDom.fadeIn().delay(2000).fadeOut();
       }
    },
    showMapControl:function(){
      var that=this;
      this.container.parent().find('.plusHandle').click(function(){
        if(that.zoom<that.maxZoom){
          that.setZoom(that.zoom+=that.zoomRate);
        }

      }).end().find('.minusHandle').click(function(){
        if(that.zoom>that.minZoom){
          that.setZoom(that.zoom-=that.zoomRate);
        }

      })
    },
    tableKindAlias:['RF','F','L'],
    tableKind:['ROOTFACT','FACT','LOOKUP'],
    //提供table类型的dom结构
    renderTableKind:function(kind){
        if(kind=='ROOTFACT'){
           return '<span class="snowFont snowFont1 tableKind">RF</span>'
        }else if(kind=='FACT'){
          return '<span class="snowFont snowFont2 tableKind" >F</span>'
        }else{
         return '<span class="snowFont snowFont3 tableKind" >L</span>'
        }
    },
    //提供列类型的dom结构
    renderTableColumnKind:function(type){
      if(type=='dimension'){
        return '<span class="snowFont snowFont5 columnKind" data="dimension">D</span>';
      }else if(type=='measure') {
        return '<span class="snowFont snowFont6 columnKind" data="measure">M</span>';
      }else if(type=='disable'){
        return '<span class="snowFont snowFont4 columnKind" data="disable">-</span>';
      }else{
        return '<span class="snowFont snowFont5 columnKind" data="dimension">D</span>';
      }
    },
    renderPartionColumn:function(column){
      var columnType=column.datatype;
      var isDate=column.isDate;
      var isTime=column.isTime;
      var canSetDatePartion=['date','timestamp','string','bigint','int','integer'];
      var canSetTimePartion=['time','timestamp','string'];
      var needNotSetDateFormat=['bigint','int','integer'];
      var domHtml='';
      var timeClass='fa fa-clock-o snowclock snowTime',dateClass='fa fa-calendar snowclock snowDate';
      if(needNotSetDateFormat.indexOf(columnType)>=0){
        timeClass+=' noshow';
        dateClass+=' noFormat';
      }
      if(canSetDatePartion.indexOf(columnType)<0&&columnType.indexOf('varchar')<0){
        dateClass+=' noshow';
      }
      if(canSetTimePartion.indexOf(columnType)<0&&columnType.indexOf('varchar')<0){
        timeClass+=' noshow';
      }
      if(isDate){
        dateClass+=' active';
      }
      if(isTime){
        timeClass+=' active';
      }
      domHtml+= '<i class="'+dateClass+'"></i>';
      domHtml+= '<i class="'+timeClass+'"></i>';
      return domHtml;
    },
    columnTypes:['dimension','measure','disable'],
    //数据仓库
    tableList:{
      data:[],
      add:function(obj,errcallback){
        var that=this;
        for(var i=0;i<this.data.length;i++){
          if(this.data[i].alias==obj.alias){
            if(typeof  errcallback=='function'){
              errcallback();
              return;
            }
          }
        }
        this.data.push($.extend(true,{},obj));
      },
      getTable:function(key,val){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i][key]==val){
            return this.data[i];
          }
        }
      },
      remove:function(key,val){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i][key]==val){
             this.data.splice(i,1);
             break;
          }
        }
      },
      update:function(key,val,obj){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i][key]==val){
            for(var s in obj){
              this.data[i][s]=obj[s];
            }
            break;
          }
        }
      },
      updateColumnKind:function(guid,columnName,kind){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i]['guid']==guid){
            for(var k=0;k<this.data[i].columns.length;k++){
              if(this.data[i].columns[k].name==columnName){
                this.data[i].columns[k].kind=kind;
                return this;
              }
            }
          }
        }
      },
      getRootFact:function(){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i]['kind']=='ROOTFACT'){
            return this.data[i];
          }
        }
      }
    },
    //刷新页面的别名显示
    refreshAlias:function(tableName,newVal){
      this.getTableDom(tableName).find('.alias').html('Alias:'+newVal);
      return this;
    },
    //刷新页面表类型显示
    refreshTableKind:function(tableName,kind){
      this.getTableDom(tableName).find('.tableKind').replaceWith(this.renderTableKind(kind)).end().removeClass('isfact').removeClass('islookup');

      if(kind=='FACT'||kind=='ROOTFACT'){
        this.getTableDom(tableName).addClass('isfact');
      }else{
        this.getTableDom(tableName).addClass('islookup');
      }
      return this;
    },
    //刷新页面表中列的类型显示
    refreshTableColumnKind:function(guid,columnName,kind){
      $('#column_'+guid+columnName).find('.columnKind').replaceWith(this.renderTableColumnKind(kind));
    },
    //页面表中列绑定点击改变事件
    bindColumnChangeTypeEvent:function(box,guid){
      var that=this;
      $(box).on('click','.columnKind',function(){
        if(that.checkLock()){
          return;
        }
        var columnName=$(this).parent().attr('data');
        var tableName=$(box).attr('data');
        var columnType=$(this).attr('data');
        var nextTypeIndex=that.columnTypes.indexOf(columnType)+1<=that.columnTypes.length-1?that.columnTypes.indexOf(columnType)+1:0;
        that.refreshTableColumnKind(guid,columnName,that.columnTypes[nextTypeIndex]);
        that.tableList.updateColumnKind(guid,columnName,that.columnTypes[nextTypeIndex]);
      })
    },
    getTableDom:function(guid){
       return  $('#umlobj_'+guid);
    },
    aliasList:[],
    checkHasThisAlias:function(alias){
      var tableDetail=this.tableList.getTable('alias',alias);
      if(tableDetail){
        return true;
      }
      return false;
    },
    //保存数据监测
    checkSaveData:function(errcallback,callback){
      var toKylinData=this.plumbDataToKylinData();
      //数据格式是否有误差
      if(typeof toKylinData=='object'){
        if(!toKylinData.fact_table){
          errcallback('norootfact');
          return;
        }
        //检察是否有dimension
        if(toKylinData.dimensions&&toKylinData.dimensions.length==0){
          errcallback('nodimension');
          return;
        }
      }
      if(typeof toKylinData=='string'){
        errcallback('formaterror');
        return;
      }
      callback(toKylinData);

    },
    renderTables:function(tableBaseObjects){
      var that=this;
      for(var tableLen= 0;tableLen<tableBaseObjects.length;tableLen++){
        (function(tableBaseObject){
          var str=' <div data="'+tableBaseObject.table+'" id="umlobj_'+tableBaseObject.guid+'" class="classUml '+(tableBaseObject.kind!='LOOKUP'?'isfact':'islookup')+'" style="left:'+tableBaseObject.pos[0]+'px;top:'+tableBaseObject.pos[1]+'px">';
          str+=' <div 	class="title" style="height:'+that.titleHeight+'px">';
          str+=that.renderTableKind(tableBaseObject.kind);
          str+='<a title="'+tableBaseObject.table+'"><i class="fa fa-table"></i> '+tableBaseObject.table+'</a><span class="more" >X</span>' +
            '<a class="alias" >Alias:'+tableBaseObject.alias+'</a><input type="text" class="input_alias""/><span class="input_alias_save">OK</span></div>';
          for (var i =0; i <tableBaseObject.columns.length; i++) {
            str+='<p  id="column_'+tableBaseObject.guid+tableBaseObject.columns[i].name+'" style="width:'+that.itemWidth+'px" data="'+tableBaseObject.columns[i].name+'">'+that.renderTableColumnKind(tableBaseObject.columns[i].kind)+'&nbsp;&nbsp;'+that.cutStr(tableBaseObject.columns[i].name,30,"...")+that.renderPartionColumn(tableBaseObject.columns[i])+'<span class="jsplumb-tips">'+tableBaseObject.columns[i].name+'('+tableBaseObject.columns[i].datatype+')</span></p>';
          }
          str+='</div>';
          $("#"+that.containerId).append($(str));
          var len=tableBaseObject.columns.length;
          var totalHeight=that.titleHeight+that.itemHeight*len;
          var totalPer=that.titleHeight/totalHeight;
          var boxIdName='umlobj_'+tableBaseObject.guid;
          var boxDom=$("#"+boxIdName);
          for (var i =0; i <len; i++) {
            var h=that.numDiv((that.titleHeight+that.itemHeight*i+that.numDiv(that.itemHeight,2)),totalHeight);
            that.instance.addEndpoint(boxIdName, {anchor:[[1.0,h, 1.5, 0],[0, h, -1, 0]]}, that.createPoint({
                parameters:{
                  data:{guid:tableBaseObject.guid,column:tableBaseObject.guid+'.'+tableBaseObject.columns[i].name,type:tableBaseObject.columns[i].type}
                },
                uuid:tableBaseObject.alias+'.'+tableBaseObject.columns[i].name
              }
            ));
            //that.instance.addEndpoint(boxIdName, {anchor:[[0,h, -1, 0],[1, h, 1.5, 0]]}, that.createPoint({
            //    parameters:{
            //      data:{column:tableBaseObject.guid+'.'+tableBaseObject.columns[i].name,type:tableBaseObject.columns[i].kind}
            //    },
            //    uuid:tableBaseObject.alias+'.'+tableBaseObject.columns[i].name
            //  }
            //));
          }
          that.bindColumnChangeTypeEvent(boxDom,tableBaseObject.guid);
          that.instance.draggable(boxDom,{
            start:function(){
              that.container.panzoom('disable');
              $(this).css("cursor","move");
            },
            drag:function(e){

            },
            stop:function(e){
              that.tableList.update('guid',tableBaseObject.guid,{'pos':e.pos})
              that.storeCache();
              that.container.panzoom('enable');
              $(this).css("cursor","");
            }
          });
          boxDom.find('.more').on('click',function(){
            if(that.checkLock()){
              return;
            }
            var tableObj=that.tableList.getTable('guid',tableBaseObject.guid);
            that.delTable(tableBaseObject);
          })
          boxDom.on('click','.snowDate',function(){
            if(that.checkLock()){
              return;
            }
            var snowDateDomList=that.container.find('.snowDate');
            var columnName=$(this).parent().attr('data');
            var guid=tableBaseObject.guid;
            var currentDom=$(this);
            var hisPartionObj={};
            if(that.partitionDate[guid]&&that.partitionDate[guid].columnName==columnName){
              hisPartionObj=that.partitionDate[guid];
            }
            if(currentDom.hasClass('noFormat')){
              that.partitionDate={}
              that.partitionDate[guid]={
                columnName:columnName,
                dateType:'yyyyMMdd'
              }
              snowDateDomList.removeClass('active');
              currentDom.addClass('active');
            }else{
              that.addColumnToPartitionDate(function(type){
                if(type=='del'){
                  that.partitionDate={};
                  currentDom.removeClass('active');
                  return;
                }
                that.partitionDate={}
                that.partitionDate[guid]={
                  columnName:columnName,
                  dateType:type
                }
                snowDateDomList.removeClass('active');
                currentDom.addClass('active');
              },{type:'date'},hisPartionObj)
            }
            that.storeCache();
          })
          boxDom.on('click','.snowTime',function(){
            if(that.checkLock()){
              return;
            }
            var snowTimeDomList=that.container.find('.snowTime');
            var columnName=$(this).parent().attr('data');
            var guid=tableBaseObject.guid;
            var currentDom=$(this);
            var hisPartionObj={};
            if(that.partitionTime[guid]&&that.partitionTime[guid].columnName==columnName){
              hisPartionObj=that.partitionTime[guid];
            }
            if(currentDom.hasClass('noFormat')){
              that.partitionTime={}
              that.partitionTime[guid]={
                columnName:columnName,
                dateType:'HHmmss'
              }
              snowTimeDomList.removeClass('active');
              currentDom.addClass('active');
            }else{
              that.addColumnToPartitionTime(function(type){
                if(type=='del'){
                  that.partitionDate={};
                  currentDom.removeClass('active');
                  return;
                }
                that.partitionTime={}
                that.partitionTime[guid]={
                  columnName:columnName,
                  dateType:type
                }
                snowTimeDomList.removeClass('active');
                currentDom.addClass('active');
              },{type:'time'},hisPartionObj)
            }
            that.storeCache();
          })

          boxDom.on('click','.tableKind',function(){
            if(that.checkLock()){
              return;
            }
            var kindAlias=$(this).html();
            var index=that.tableKindAlias.indexOf(kindAlias);
            if(index>=that.tableKindAlias.length-1){
              index=0;
            }else{
              index++;
            }
            var willKind=that.tableKind[index];
            if(willKind=='ROOTFACT'&&(that.tableList.getRootFact()||that.getForeignKeyCount(tableBaseObject.guid)>0)){
              index++;
            }
            $(this).replaceWith(that.renderTableKind(that.tableKind[index])) ;
            that.tableList.update('guid',tableBaseObject.guid,{
              kind:that.tableKind[index]
            });
            that.refreshTableKind(tableBaseObject.guid,that.tableKind[index]);
            if(that.tableKind[index]=='ROOTFACT'){
              that.tableList.update('guid',tableBaseObject.guid,{
                alias:tableBaseObject.name
              });
              boxDom.find('.input_alias').val(tableBaseObject.name).hide();
              that.refreshAlias(tableBaseObject.guid,tableBaseObject.name);
            }
            that.storeCache();
          })
          var aliasLabel='Alias:';
          boxDom.on('dblclick','.alias',function(){
            if(that.checkLock()){
              return;
            }
            var rootFact=that.tableList.getRootFact();
            if(rootFact&&rootFact.guid==tableBaseObject.guid){
              that.showTips('rootaliaslimit');
              return;
            }
            $(this).next().show().focus().val($(this).html().replace(aliasLabel,'')).select();
          });
          boxDom.on('blur','.input_alias',function(){
            if(that.checkLock()){
              return;
            }
            $(this).hide();
            var aliasInputVal=$(this).val().toUpperCase();
            var labelDom=$(this).prev();
            if(labelDom.html().replace(aliasLabel,'')!=aliasInputVal&&that.checkHasThisAlias(aliasInputVal)){
              that.showTips('samealias');
            }else{
              $(this).hide();
              labelDom.show().html(aliasLabel+that.filterSpecialChar(aliasInputVal));
              that.tableList.update('guid',tableBaseObject.guid,{
                alias:that.filterSpecialChar(aliasInputVal)
              });
            }
            that.storeCache();
          });
          boxDom.on('dbclick','.input_alias',function(){
            if(that.checkLock()){
              return;
            }
            $(this).focus().select();
          })
          boxDom.find('.input_alias').mousemove(function(e){
            e.stopPropagation();
          })
          boxDom.find('.input_alias,p').mousedown(function(e){
            e.stopPropagation();
          })
          $('body').on('click',function(e){
            that.container.parent().find('.tableColumnInfoBox').fadeOut();
            if(!$(e.target).hasClass('input_alias')){
              that.container.find('.input_alias:visible').blur();
            }
          })
        }(tableBaseObjects[tableLen]));
      }
      this.storeCache();
      return this;
    },
    //添加表
    addTable:function(tableData,offset){
      var that=this;
      var basePosition=that.getBasePosition();
      var tableBaseObject={
        name:tableData.name,
        table:tableData.table||tableData.database+'.'+tableData.name,
        alias:tableData.alias||tableData.name,
        kind:tableData.kind||'LOOKUP',
        columns: $.extend(true,[],tableData.columns),
        pos:[basePosition[0]+offset[0],basePosition[1]+offset[1]],
        guid:tableData.guid||this.guid()

      };
      this.tableList.add(tableBaseObject);
      this.renderTables([tableBaseObject]);
      return this;
    },

    addTables:function(tableDatas){
      var len=tableDatas&&tableDatas.length||0;
      for(var i=0;i<len;i++){
        this.addTable(tableDatas[i],i);
      }
    },
    removeTable:function(guid){
      $("#umlobj_"+guid).remove();
      this.tableList.remove('guid',guid);
      this.instance.removeAllEndpoints("umlobj_"+guid);
    },
    //位置计算层级树算法
    calcPosition:function(){
      var that=this;
      for(var i=0;i<this.tableList.data.length;i++){
        this.tableList.data[i].size=[that.itemWidth,this.tableList.data[i].columns.length*that.itemHeight+that.titleHeight]
      }
      var factTable=that.tableList.getRootFact();
      var factPosition={};
      factPosition[factTable.guid]=factTable.size;
      var layerArr=[[factPosition]];
      calcLayer([factTable.guid]);
      function calcLayer(guidList){
        var arr=[],goOnCalc=[];
        for(var k=0;k<guidList.length;k++){
          var tableDetail=that.tableList.getTable('guid',guidList[k]);
          for(var i in that.connects){
            if(that.connects[i][1].indexOf(guidList[k])==0){
              var obj={};
              var primaryTableGuid=that.connects[i][0].split('.')[0];
              obj[primaryTableGuid]=that.tableList.getTable('guid',primaryTableGuid).size;
              goOnCalc.push(primaryTableGuid);
              arr.push(obj)
            }
          }
        }
        if(goOnCalc.length){
          layerArr.push(arr);
          calcLayer(goOnCalc);
        }
      }
      var basePosition=that.getBasePosition(100);
      var maxHeightInGrid=[],boxMarginL=120,boxMarginT=60;
      for(var m=0;m<layerArr.length;m++){
        var currLayer=layerArr[m];
        var lockGridIndex=1;
        for(var n=0;n<layerArr[m].length;n++){
          for(var s in layerArr[m][n]){
            var boxH;
            //if(maxHeightInGrid[n]){
              var level2Base=basePosition[1]+factTable.size[1];
              var lastH=(maxHeightInGrid[n]||level2Base)+boxMarginT;
              var pos;
              var currentH=that.tableList.getTable('guid',s).size[1];

              if(n>=1){
                if(maxHeightInGrid[lockGridIndex]+currentH<=maxHeightInGrid[lockGridIndex-1]){
                  pos=[(lockGridIndex)*(that.itemWidth+boxMarginL)+basePosition[0],(maxHeightInGrid[lockGridIndex]||level2Base)+boxMarginT];
                  maxHeightInGrid[lockGridIndex]=lastH+currentH;
                }else{
                  lockGridIndex++;
                  pos=[(lockGridIndex)*(that.itemWidth+boxMarginL)+basePosition[0],(maxHeightInGrid[lockGridIndex]||level2Base)+boxMarginT];
                }
              }else{
                if(m==0){
                  pos=[(n+1)*(that.itemWidth+boxMarginL)+basePosition[0],(maxHeightInGrid[n]||basePosition[1])+boxMarginT];
                  maxHeightInGrid[n+1]=lastH;
                }else{
                  pos=[(n)*(that.itemWidth+boxMarginL)+basePosition[0],(maxHeightInGrid[n]||level2Base)+boxMarginT];
                  maxHeightInGrid[n]=lastH+currentH;
                }

              }
              that.tableList.update('guid',s,{
                pos:pos
              })

          }
        }
      }
      for(var i= 0;i<that.tableList.data.length;i++){
        $('#umlobj_'+that.tableList.data[i].guid).css({
          top:that.tableList.data[i].pos[1]+'px',
          left:that.tableList.data[i].pos[0]+'px',
        })
      }
      that.instance.setSuspendDrawing(false, true)
    },
    getBasePosition:function(offsetSize){
      offsetSize=offsetSize||0;
      var transSize=$("#snowBox").css('transform');
      var x=0,y=0;
      if(transSize){
        x=parseFloat(transSize.split(',')[4])||0;
        y=parseFloat(transSize.split(',')[5])||0
      }
      return [-parseInt(this.container.css('left'))+offsetSize-x,-parseInt(this.container.css('top'))+offsetSize-y]
    },
    beginDrop:false,
    //创建连接点
    createPoint:function(para){
      var that=this;
      var pointObj = {
        endpoint:"Rectangle",//设置连接点的形状为圆形
        paintStyle:{ fill:'#46b8da',width: 10, height: 10 },//设置连接点的颜色
        isSource:true,	//是否可以拖动（作为连线起点）
        scope:"green dot",//连接点的标识符，只有标识符相同的连接点才能连接
        connector: ["Bezier", { curviness:163 } ],//设置连线为贝塞尔曲线
        maxConnections:100,//设置连接点最多可以连接几条线
        isTarget:true,	//是否可以放置（作为连线终点）
        connectorStyle: {
          strokeWidth: 5,
          stroke: '#66a8fa'
        },
        dropOptions:{
          hoverClass:"dropHover",//释放时指定鼠标停留在该元素上使用的css class
          activeClass:"dragActive",//设置放置相关的css
        },
        beforeDetach:function(conn) {	//绑定一个函数，在连线前弹出确认框
           delete that.connects[info.connection.id];
        },
        reattachConnections:function(){
          // alert(1);
        },
        onMaxConnections:function(info) {//绑定一个函数，当到达最大连接个数时弹出提示框
          // alert("Cannot drop connection " + info.connection.id + " : maxConnections has been reached on Endpoint " + info.endpoint.id);
        },
        onConnectionDetached:function(){
          // alert(2);
        },
        beforeDrop:function(){
          that.beginDrag=true;
          return true;
        }
      };
      return $.extend(pointObj,para);
    },
    //主动连接
    connect:function(p1,p2,otherProper){
       var defaultPata={uuids: [p1,p2], editable: true};
       $.extend(defaultPata,otherProper);
       this.instance.connect(defaultPata);
    },
    //缩放空间
    setZoom:function(zoom, transformOrigin, el) {
      transformOrigin = transformOrigin || [ 0.5, 0.5 ];
      this.instance = this.instance || jsPlumb;
      el = el || this.instance.getContainer();
      var p = [ "webkit", "moz", "ms", "o" ],
        s = "scale(" + zoom + ")",
        oString = (transformOrigin[0] * 100) + "% " + (transformOrigin[1] * 100) + "%";

      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + "Transform"] = s;
        el.style[p[i] + "TransformOrigin"] = oString;
      }

      el.style["transform"] = s;
      el.style["transformOrigin"] = oString;

      this.instance.setZoom(zoom);
    },
    /*
    * ＝＝＝＝＝＝＝＝＝＝工具函数＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
    *
    * */
    numDiv:function(num1, num2) {
      var baseNum1 = 0, baseNum2 = 0;
      var baseNum3, baseNum4;
      try {
        baseNum1 = num1.toString().split(".")[1].length;
      } catch (e) {
        baseNum1 = 0;
      }
      try {
        baseNum2 = num2.toString().split(".")[1].length;
      } catch (e) {
        baseNum2 = 0;
      }
      baseNum3 = Number(num1.toString().replace(".", ""));
      baseNum4 = Number(num2.toString().replace(".", ""));
      return (baseNum3 / baseNum4) * Math.pow(10, baseNum2 - baseNum1);
    },
    guid:function(){
      function S4() {
        return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
      }
      function guid() {
        return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
      }
      return guid();
    },
    isNullObj:function(obj){
       for(var i in obj){
         return false;
       }
       return true;
    },
    filterSpecialChar:function(str){
      if(str){
        return str.replace(/[.]/g,'');
      }
      return '';
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
  return $.extend(true,{initService:function(){
    return $.extend(true,{},DrawHelper);
  }},DrawHelper);
});
