//snow model design
KylinApp.factory('DrawHelper', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
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
    zoomRate:0.01,
    changeTableInfo:null,
    changeConnectType:null,
    addColumnToPartitionDate:null,
    addColumnToPartitionTime:null,
    saveModel:null,
    instanceName:'',
    instanceDiscribe:'',
    filterStr:'',
    containerSize:{
      width:100000,
      height:100000
    },
    init:function(para){
      $.extend(this,para);
      this.boxWidth=$('#'+this.containerId).width();
      this.boxHeight=$('#'+this.containerId).height();
      var that=this;
      this.container=$('#'+this.containerId);
      this.container.css({width:this.containerSize.width+'px',height:this.containerSize.height+'px',left:(-this.containerSize.width+this.container.parent().width())/2+'px',top:(-this.containerSize.height+this.container.parent().height())/2+'px'})
      this.container.parent().css({'height':this.boxHeight+'px','overflow':'hidden'});
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
      var that=this;
      this.instance.bind("connection", function (info, originalEvent) {
        that.lastLink.push(info.connection.getParameters().data);
        //自己不能连自己
        if(info.sourceId==info.targetId){
          that.instance.detach(info.connection);
          return;
        }
        //rootFact不能连别人
        var rootFactTable=that.tableList.getRootFact();
        if(that.lastLink[0]&&rootFactTable&&rootFactTable.guid==that.lastLink[0].column.split('.')[0]){
          that.instance.detach(info.connection);
          return;
        }
        //不同类型的主外键关联预警
        if(that.lastLink[0].type!=that.lastLink[1].type){

        }

        that.connects[info.connection.id]=[that.lastLink[0].column,that.lastLink[1].column];
        that.changeConnectType(info.connection);
        that.plumbDataToKylinData();
        info.connection.unbind('click').bind('click',function(conn){
          if(conn.id!='label'){
            that.changeConnectType(conn);
          }
        })
      });
      var $panzoom=this.container.panzoom({
        cursor: "zoom",
        minScale: 0.25,
        increment: 0.1,
        duration: 100
      });
      this.showToolbar();
      this.showMapControl();
      this.showActionBtn();
      return this;
    },
    kylinDataToJsPlumbData:function(){

    },
    //操作台数据转换为kylin接受数据
    plumbDataToKylinData:function(){
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
       if(!rootFactTable){
         return 'lose root fact';
       }
       kylinData.fact_table=rootFactTable.table;
      //采集jontable的信息
       var lookups={},linkTables={};
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
             tableObj.columns.push(tableBase.alias+'.'+tableBase.columns[m].name)
           }else if('measure'==tableBase.columns[m].kind){
             kylinData.metrics.push(tableBase.alias+'.'+tableBase.columns[m].name);
           }
        }
        if(tableObj.columns.length>0){
          kylinData.dimensions.push(tableObj);
        }
      }
      console.log(kylinData);
      return kylinData;
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
    showToolbar:function(){
       var toolBarHtml='<div id="tipToolbar"><span class="snowFont snowFont1 relative">RF</span><span>Root Fact Table</span><span class="snowFont snowFont2 relative">F</span><span>Fact Table</span><span class="snowFont snowFont3 relative">L</span><span>Lookup Table</span><span class="snowFont snowFont5 relative">D</span><span>Dimension</span><span class="snowFont snowFont6 relative">M</span><span>Measure</span><span class="snowFont snowFont4 relative">-</span><span>Disable</span></div>';
       $(toolBarHtml).insertAfter(this.container);
    },
    showActionBtn:function(){
      var that=this;
      var actionBar='<div id="bar_action"><span>Save</span><span>JSON</span></div>';
      $(actionBar).insertAfter(this.container);
      $('#bar_action').click(function(){
        that.saveModel();
      })
    },
    showMapControl:function(){
      var that=this;
      var mapControlHtml='<span class="plusHandle"></span><span class="minusHandle"></span>';
      $(mapControlHtml).insertAfter(this.container);
      this.container.parent().find('.plusHandle').click(function(){
        if(that.zoom<1.2){
          that.setZoom(that.zoom+=that.zoomRate);
        }

      }).end().find('.minusHandle').click(function(){
        if(that.zoom>0.43){
          that.setZoom(that.zoom-=that.zoomRate);
        }

      })

    },
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
    renderPartionColumn:function(columnType){
      var canSetDatePartion=['date','timestamp','string','bigint','int','integer'];
      var canSetTimePartion=['time','timestamp','string'];
      var needNotSetDateFormat=['bigint','int','integer'];
      var domHtml='';
      if(canSetDatePartion.indexOf(columnType)>=0&&canSetTimePartion.indexOf(columnType)>=0||columnType.indexOf('varchar')>=0){
        if(needNotSetDateFormat.indexOf(columnType)>=0){
          domHtml+= '<i class="fa fa-calendar snowclock noFormat snowDate" ></i>';
          domHtml+= '<i class="fa fa-clock-o snowclock  noFormat snowTime" ></i>';
        }else{
          domHtml+= '<i class="fa fa-calendar snowclock snowDate" ></i>';
          domHtml+= '<i class="fa fa-clock-o snowclock  snowTime" ></i>';
        }
      }else if(canSetDatePartion.indexOf(columnType)>=0||columnType.indexOf('varchar')>=0){
        if(needNotSetDateFormat.indexOf(columnType)>=0){
          domHtml+= '<i class="fa fa-calendar snowclock noFormat snowDate" ></i>';
          domHtml+= '<i class="fa fa-clock-o snowclock noshow noFormat snowTime" ></i>';
        }else{
          domHtml+= '<i class="fa fa-calendar snowclock snowDate" ></i>';
          domHtml+= '<i class="fa fa-clock-o snowclock noshow snowTime" ></i>';
        }
      }else if(canSetTimePartion.indexOf(columnType)>=0||columnType.indexOf('varchar')>=0){
        if(needNotSetDateFormat.indexOf(columnType)>=0){
          domHtml+= '<i class="fa fa-clock-o snowclock noFormat snowTime"></i>';
          domHtml+= '<i class="fa fa-calendar snowclock noshow noFormat snowDate" ></i>';
        }else{
          domHtml+= '<i class="fa fa-clock-o snowclock snowTime"></i>';
          domHtml+= '<i class="fa fa-calendar snowclock noshow snowDate" ></i>';
        }
      }
      return domHtml;
    },
    columnTypes:['dimension','measure','disable'],
    //数据仓库
    tableList:{
      data:[],
      add:function(obj,errcallback){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i].alias==obj.alias){
            if(typeof  errcallback=='function'){
              errcallback();
              return;
            }
          }
        }
        this.data.push(obj);
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
            $.extend(this.data[i],obj);
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

      if(kind=='fact'||kind=='rootfact'){
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
      if(alias&&this.aliasList.indexOf(alias)==-1){
        return false;
      }
      return true;
    },
    changeAliasList:function(oldAlias,newAlias){
      var index=this.aliasList.indexOf(oldAlias);
      if(index>=0){
        this.aliasList.splice(index,1,newAlias);
      }else{
        this.aliasList.push(newAlias);
      }

    },
    //添加表
    addTable:function(tableData,count){
      var that=this;
      var tableBaseObject={
        table:tableData.database+'.'+tableData.name,
        alias:tableData.alias||tableData.name,
        kind:tableData.kind||'LOOKUP',
        columns: $.extend(true,[],tableData.columns),
        pos:that.calcPosition(),
        guid:tableData.guid||this.guid()
      };
      this.tableList.add(tableBaseObject);
      var str=' <div data="'+tableData.database+'.'+tableData.name+'" id="umlobj_'+tableBaseObject.guid+'" class="classUml '+(tableBaseObject.kind!='LOOKUP'?'isfact':'islookup')+'" style="left:'+tableBaseObject.pos[0]+'px;top:'+tableBaseObject.pos[1]+'px">';
          str+=' <div 	class="title" style="height:'+this.titleHeight+'px">';
          str+=this.renderTableKind(tableBaseObject.kind);
          str+='<a title="'+tableBaseObject.table+'"><i class="fa fa-table"></i> '+tableBaseObject.table+'</a><span class="more" ></span>' +
                    '<a class="alias" title="'+tableBaseObject.table+'">Alias:'+tableBaseObject.alias+'</a></div>';
          for (var i =0; i <tableData.columns.length; i++) {
            str+='<p  id="column_'+tableBaseObject.guid+tableData.columns[i].name+'" style="width:'+that.itemWidth+'px" data="'+tableData.columns[i].name+'">'+this.renderTableColumnKind(tableData.columns[i].type)+'&nbsp;&nbsp;'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')'+this.renderPartionColumn(tableData.columns[i].datatype)+'<span class="jsplumb-tips">'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')</span></p>';
          }
          str+='</div>';
      $("#"+this.containerId).append($(str));
      var len=tableData.columns.length;
      var totalHeight=this.titleHeight+this.itemHeight*len;
      var totalPer=this.titleHeight/totalHeight;
      var boxIdName='umlobj_'+tableBaseObject.guid;
      var boxDom=$("#"+boxIdName);
      for (var i =0; i <len; i++) {
        var h=this.numDiv((this.titleHeight+this.itemHeight*i+this.numDiv(this.itemHeight,2)),totalHeight);
        this.instance.addEndpoint(boxIdName, {anchor:[[1.0,h, 1.5, 0],[0, h, -1, 0]]}, this.createPoint({
            parameters:{
              data:{column:tableBaseObject.guid+'.'+tableBaseObject.columns[i].name,type:tableData.columns[i].type}
            },
            uuid:tableBaseObject.guid
          }
        ));
        this.instance.addEndpoint(boxIdName, {anchor:[[0,h, -1, 0],[1, h, 1.5, 0]]}, this.createPoint({
            parameters:{
              data:{column:tableBaseObject.guid+'.'+tableBaseObject.columns[i].name,type:tableData.columns[i].type}
            },
            uuid:tableBaseObject.guid
          }
        ));
      }
      this.bindColumnChangeTypeEvent(boxDom,tableBaseObject.guid);
      this.instance.draggable(boxDom,{
        drag:function(e){
        },stop:function(e){
          that.tableList.update('guid',tableBaseObject.guid,{'pos':e.pos})
        }
      });
      boxDom.find('.more').on('click',function(){
        var tableObj=that.tableList.getTable('guid',tableBaseObject.guid);
        that.changeTableInfo(tableBaseObject);
      })
      boxDom.on('click','.snowDate',function(){
        var snowDateDomList=that.container.find('.snowDate');
        var columnName=$(this).parent().attr('data');
        var guid=tableBaseObject.guid;
        var currentDom=$(this);
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
          },{type:'date'})
        }
      })
      boxDom.on('click','.snowTime',function(){
        var snowTimeDomList=that.container.find('.snowTime');
        var columnName=$(this).parent().attr('data');
        var guid=tableBaseObject.guid;
        var currentDom=$(this);
        if(currentDom.hasClass('noFormat')){
          that.partitionTime={}
          that.partitionTime[guid]={
            columnName:columnName,
            dateType:'yyyyMMdd'
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
          },{type:'time'})
        }

      })
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
      this.instance.removeAllEndpoints("umlobj_"+guid)
    },
    calcPosition:function(){
      var per=this.boxWidth/(this.itemWidth*1.5);
      var that=this;
        return [
          -parseInt(that.container.css('left'))+100,
          -parseInt(that.container.css('top'))+100,
        ]
    },
    //创建连接点
    createPoint:function(para){
      var that=this;
      var pointObj = {
        endpoint:"Rectangle",//设置连接点的形状为圆形
        paintStyle:{ fill:'#46b8da',width: 10, height: 10 },//设置连接点的颜色
        isSource:true,	//是否可以拖动（作为连线起点）
        scope:"green dot",//连接点的标识符，只有标识符相同的连接点才能连接
        connector: ["Bezier", { curviness:63 } ],//设置连线为贝塞尔曲线
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
        overlays:[
          [ "Label", {
            location:-130,
            label:"",
            cssClass:"endpointSourceLabel"
          }],
          'Arrow'
        ],
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
        connection:function(){
          // alert(2);
        },
        beforeDrop: function (params) {
          that.lastLink=[];
          that.lastLink.push(params.connection.getParameters().data);
          return true;
        },dropOptions:{
          drop:function(e, ui) {
            alert('drop!');
          }
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
    }
  }
});
