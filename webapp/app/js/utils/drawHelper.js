KylinApp.factory('DrawHelper', function ($modal, $timeout, $location, $anchorScroll, $window) {
  return {
    titleHeight:40,
    itemHeight:20,
    itemWidth:200,
    boxWidth:800,
    boxHeight:1000,
    baseTop:10,
    levelTopList:[10],
    containerId:'snowBox',
    tableCount:1,
    instance:null,
    zoom:1,
    init:function(para){
      $.extend(this,para);
      this.boxWidth=$("#"+this.containerId).width();
      this.boxHeight=$("#"+this.containerId).height();
      var that=this;
      this.instance = jsPlumb.getInstance({
        DragOptions: { cursor: 'pointer', zIndex: 2000 },
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
        // alert(that.lastLink);
      });
      var $panzoom=$("#"+this.containerId).panzoom({
        cursor: "default",
        minScale: 0.25,
        increment: 0.1,
        duration: 100
      });
      return this;
    },
    addTable:function(tableData,count){
      var pos=this.calcPosition();
      var str=' <div id="umlobj_'+tableData.database+tableData.name+'" class="classUml" style="left:'+pos.left+'px;top:'+pos.top+'px">';
      str+=' <div 	class="title"><div><i class="fa fa-table"></i> '+tableData.database+'.'+tableData.name+'</div><div class="alias">Alias:'+tableData.database+'.'+tableData.name+'</div></div>';
      for (var i =0; i <tableData.columns.length; i++) {
        str+='<p>+'+tableData.columns[i].name+'('+tableData.columns[i].datatype+')</p>'
      }
      str+='</div>';
      $("#"+this.containerId).append($(str));
      var len=tableData.columns.length;
      var totalHeight=this.titleHeight+this.itemHeight*len;
      var totalPer=this.titleHeight/totalHeight;
      for (var i =0; i <len; i++) {
        var h=this.numDiv((this.titleHeight+this.itemHeight*i+this.numDiv(this.itemHeight,2)),totalHeight);
        this.instance.addEndpoint('umlobj_'+tableData.database+tableData.name, {anchor:[[1.0,h, 1.5, 0],[0, h, -1, 0]]}, this.createPoint({
            parameters:{
              data:tableData.columns[i].columnName
            },
            uuid:tableData.database+tableData.name+tableData.columns[i].columnName
          }
        ));
      }
      this.instance.draggable($("#umlobj_"+tableData.database+tableData.name));
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
    lastLink:[],
    createPoint:function(para){
      var that=this;
      var pointObj = {
        endpoint:["Dot", { radius:5 }],//设置连接点的形状为圆形
        paintStyle:{ fillStyle:'#46b8da' },//设置连接点的颜色
        isSource:true,	//是否可以拖动（作为连线起点）
        scope:"green dot",//连接点的标识符，只有标识符相同的连接点才能连接
        connectorStyle:{ strokeStyle:'#46b8da', lineWidth:3 },//连线颜色、粗细
        connector: ["Bezier", { curviness:63 } ],//设置连线为贝塞尔曲线
        maxConnections:1,//设置连接点最多可以连接几条线
        isTarget:true,	//是否可以放置（作为连线终点）
        overlays: [
          "Arrow"
        ],
        dropOptions:{
          hoverClass:"dropHover",//释放时指定鼠标停留在该元素上使用的css class
          activeClass:"dragActive",//设置放置相关的css
        },
        beforeDetach:function(conn) {	//绑定一个函数，在连线前弹出确认框
          return confirm("Detach connection?");
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
