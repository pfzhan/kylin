//snow model design
KylinApp.factory('DrawHelper', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
    titleHeight:60,
    itemHeight:20,
    itemWidth:200,
    boxWidth:800,
    boxHeight:1000,
    baseTop:10,
    levelTopList:[10],
    containerId:'snowBox',
    tableCount:1,
    instance:null,
    lastLink:[],
    connects:{},
    zoom:1,
    changeTableInfo:null,
    init:function(para){
      $.extend(this,para);
      this.boxWidth=$("#"+this.containerId).width();
      this.boxHeight=$("#"+this.containerId).height();
      var that=this;
      this.instance = jsPlumb.getInstance({
          DragOptions: { cursor: 'pointer', zIndex: 2000},
          PaintStyle: { strokeStyle: '#66a8fa' },
          EndpointHoverStyle: { fillStyle: "yellow" },
          HoverPaintStyle: { strokeStyle: "yellow" },
          EndpointStyle: { width: 20, height: 16, strokeStyle: '#66a8fa' },
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
        that.connects[info.connection.id]=that.lastLink;
        info.connection.bind('dblclick',function(conn){
          that.instance.detach(conn);
          delete that.connects[conn.id];
        })
      });
      var $panzoom=$("#"+this.containerId).panzoom({
        cursor: "default",
        minScale: 0.25,
        increment: 0.1,
        duration: 100
      });
      return this;
    },
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
      remove:function(key,val){
        for(var i=0;i<this.data.length;i++){
          if(this.data[i][key]==val){
            delete this.data[i];
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
      }
    },
    refreshAlias:function(tableName,newVal){
      this.getTableDom(tableName).find('.alias').html('Alias:'+newVal);
      return this;
    },
    getTableDom:function(tableName){
       return  $('#umlobj_'+tableName);
    },
    addTable:function(tableData,count){
      var tableBaseObject={
        table:tableData.database+'.'+tableData.name,
        alias:tableData.name,
        kind:tableData.kind||'lookup',
        isRoot:tableData.isRoot||false,
        pos:[10,10]
      };
      var that=this;
      this.tableList.add(tableBaseObject);
      var str=' <div id="umlobj_'+tableData.database+tableData.name+'" class="classUml" style="left:'+tableBaseObject.pos[0]+'px;top:'+tableBaseObject.pos[1]+'px">';
          str+=' <div 	class="title" style="height:'+this.titleHeight+'px">';
          if(tableBaseObject.kind=='fact'){
            if(tableBaseObject.isRoot){
                str+='<span class="snowFont snowFont1">RF</span>'
            }else{
               str+='<span class="snowFont snowFont2">F</span>'
            }
          }else{
              str+='<span class="snowFont snowFont3">L</span>'
          }
          str+='<a title="'+tableBaseObject.table+'"><i class="fa fa-table"></i> '+tableBaseObject.table+'<span class="more" ></span></a>' +
                    '<a class="alias" title="'+tableBaseObject.table+'">Alias:'+tableBaseObject.alias+'</a></div>';
          for (var i =0; i <tableData.columns.length; i++) {
            if(tableData.columns[i].type=='dimension'){
              str+='<span class="snowFont snowFont2">D</span><p>&nbsp;&nbsp;'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')</p>';
            }else if(tableData.columns[i].type=='measure') {
              str+='<span class="snowFont snowFont3">M</span><p>&nbsp;&nbsp;'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')</p>';
            }else if(tableData.columns[i].type=='disable'){
              str+='<span class="snowFont snowFont4">-</span><p class="snowFont4">&nbsp;&nbsp;'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')</p>';
            }else{
              str+='<span class="snowFont snowFont2">D</span><p>&nbsp;&nbsp;'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')</p>';
            }

          }
          str+='</div>';
      $("#"+this.containerId).append($(str));
      var len=tableData.columns.length;
      var totalHeight=this.titleHeight+this.itemHeight*len;
      var totalPer=this.titleHeight/totalHeight;
      for (var i =0; i <len; i++) {
        var h=this.numDiv((this.titleHeight+this.itemHeight*i+this.numDiv(this.itemHeight,2)),totalHeight);
        this.instance.addEndpoint('umlobj_'+tableData.database+tableData.name, {anchor:[[0, h, -1, 0]]}, this.createPoint({
            parameters:{
              data:tableData.alias+'.'+tableData.columns[i].name
            },
            uuid:tableData.database+tableData.name+tableData.columns[i].columnName
          }
        ));
        this.instance.addEndpoint('umlobj_'+tableData.database+tableData.name, {anchor:[[1.0,h, 1.5, 0]]}, this.createPoint({
            parameters:{
              data:tableData.alias+'.'+tableData.columns[i].name
            },
            uuid:tableData.database+tableData.name+tableData.columns[i].columnName
          }
        ));
      }
      this.instance.draggable($("#umlobj_"+tableData.database+tableData.name),{
        drag:function(e){
        },stop:function(e){
          this.tableList.data.update('table',tableData.database+'.'+tableData.name,{'pos':e.pos})
        }
      });
      $('#umlobj_'+tableData.database+tableData.name).find('.more').on('click',function(){
        that.changeTableInfo(tableData);
      })
      return this;
    },
    addTables:function(tableDatas){
      var len=tableDatas&&tableDatas.length||0;
      for(var i=0;i<len;i++){
        this.addTable(tableDatas[i],i);
      }
    },
    calcPosition:function(){
      var per=this.boxWidth/(this.itemWidth*1.5);
      var that=this;
      if(this.tableCount==1){
        return {
          top:that.baseTop,
          left:(that.boxWidth-that.itemWidth)/2,
        }
      }
      return {
        top:10,
        left:10,
      }
    },

    createPoint:function(para){
      var that=this;
      var pointObj = {
        endpoint:"Rectangle",//设置连接点的形状为圆形
        paintStyle:{ fillStyle:'#46b8da',width: 10, height: 10 },//设置连接点的颜色
        isSource:true,	//是否可以拖动（作为连线起点）
        scope:"green dot",//连接点的标识符，只有标识符相同的连接点才能连接
        connectorStyle:{ strokeStyle:'#46b8da', lineWidth:5 },//连线颜色、粗细
        connector: ["Bezier", { curviness:63 } ],//设置连线为贝塞尔曲线
        maxConnections:100,//设置连接点最多可以连接几条线
        isTarget:true,	//是否可以放置（作为连线终点）
        overlays: ["Arrow"],
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
    connect:function(p1,p2){
       this.instance.connect({uuids: [p1,p2], editable: true});
    },
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
    }
  }
});
