<template>
    <div id="model_edit">
      <div class="table_box" v-for="table in tableList" :id="table.guid" >
        <span class="tool_box"><i class="el-icon-setting"></i></span>
        <p class="table_name">{{table.alias||table.name}}</p>
        <div class="link_box" v-if="selectColumn[table.guid]"><i class="el-icon-close" v-on:click="cancelFilterColumn(table.guid)"></i>{{selectColumn[table.guid]&&selectColumn[table.guid].columnName}}</div>
        <p class="filter_box"><el-input v-model="table.filterName" v-on:change="filterColumnByInput(table.filterName,table.guid)"  size="small" placeholder="enter filter..."></el-input></p>
        <section data-scrollbar class="columns_box">
          <ul>
            <li v-on:click="selectFilterColumn(table.guid,column.name,column.datatype)" v-for="column in table.columns" :key="column.guid" :class="column.name" class="column_li"><span class="kind">D</span><span class="column">{{column.name}}</span><span class="column_type">{{column.datatype}}</span></style></li>
          </ul>
        </section>
        <div class="more_tool"><i class="el-icon-d-caret"></i></div>
      </div>
    </div>
</template>
<script>
import { jsPlumb } from 'jsplumb'
import { sampleGuid } from '../../util/index'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
export default {
  data () {
    return {
      selectColumn: {},
      plumbInstance: null,
      plumbInstanceForShowLink: null,
      links: [],
      tableList: [{
        'filterName': '',
        'name': 'test1',
        'database': 'default',
        'alias': 'User_Profile',
        'guid': sampleGuid(),
        'columns': [{'id': '1', 'name': 'A', 'datatype': 'bigint'}, {'id': '2', 'name': 'B', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'TEST', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }, {
        'name': 'test2',
        'database': 'default',
        'alias': 'Product',
        'guid': sampleGuid(),
        'columns': [{'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }, {
        'name': 'test22',
        'database': 'default',
        'alias': 'Product',
        'guid': sampleGuid(),
        'columns': [{'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }],
      endpointConfig: function (type) {
        var connectorPaintStyle = {
          strokeWidth: 2,
          stroke: '#61B7CF',
          joinstyle: 'round',
          outlineStroke: 'white',
          outlineWidth: 2
        }
    // .. and this is the hover style.
        var connectorHoverStyle = {
          strokeWidth: 3,
          stroke: '#216477',
          outlineWidth: 5,
          outlineStroke: 'white'
        }
        var endpointHoverStyle = {
          fill: '#216477',
          stroke: '#216477'
        }
        var sourceEndpoint = {
          endpoint: 'Dot',
          paintStyle: {
            stroke: '#7AB02C',
            fill: 'transparent',
            radius: 7,
            strokeWidth: 1
          },
          isSource: true,
          connector: [ 'Flowchart', { stub: [40, 60], gap: 10, cornerRadius: 5, alwaysRespectStubs: true } ],
          connectorStyle: connectorPaintStyle,
          hoverPaintStyle: endpointHoverStyle,
          connectorHoverStyle: connectorHoverStyle,
          dragOptions: {},
          overlays: [
            [ 'Label', {
              location: [0.5, 1.5],
              label: 'Drag',
              cssClass: 'endpointSourceLabel',
              visible: false
            } ]
          ]
        }
    // the definition of target endpoints (will appear when the user drags a connection)
        var targetEndpoint = {
          endpoint: 'Dot',
          paintStyle: { fill: '#7AB02C', radius: 7 },
          hoverPaintStyle: endpointHoverStyle,
          maxConnections: -1,
          dropOptions: { hoverClass: 'hover', activeClass: 'active' },
          isTarget: true,
          overlays: [
                [ 'Label', { location: [0.5, -0.5], label: 'Drop', cssClass: 'endpointTargetLabel', visible: false } ]
          ]
        }
        if (type === 'source') {
          return sourceEndpoint
        } else {
          return targetEndpoint
        }
      }
    }
  },
  beforeDestroy () {
    this.removeAllEndpoints(this.plumbInstance)
  },
  methods: {
    /*
    *  Filter Func
    *  ==========================================================================
    */
    filterColumnByInput: function (filter, id) {
      var instance = Scrollbar.get($('#' + id).find('.columns_box')[0])
      var suggestObj = this.getColumnDataByLikeFilter(id, filter)
      this.autoScroll(instance, suggestObj.index * 30, suggestObj.className, id)
      this.selectFilterColumn(id, suggestObj.className, suggestObj.columnType, 'target')
    },
    cancelFilterColumn: function (id) {
      this.$set(this.selectColumn, id, '')
      // this.removeAllEndpoints(this.plumbInstance)
    },
    selectFilterColumn: function (id, columnName, columnType, pointType) {
      pointType = pointType || 'source'
      if (columnName) {
        this.$set(this.selectColumn, id, {columnName: columnName, columnType: columnType})
        this.addSelectPoints(id, this.plumbInstance, pointType, columnName, columnType)
      } else {
        this.cancelFilterColumn(id)
      }
    },
    getColumnDataByLikeFilter: function (guid, filter) {
      var suggest = {
        className: '',
        columnType: '',
        index: 0
      }
      if (filter === '') {
        return suggest
      }
      this.tableList.forEach(function (table) {
        if (table.guid === guid) {
          for (let i = 0; i < table.columns.length; i++) {
            var col = table.columns[i]
            if (col.name.toUpperCase().indexOf(filter.toUpperCase()) >= 0) {
              suggest.className = col.name
              suggest.index = i
              suggest.columnType = col.datatype
              break
            }
          }
        }
      })
      return suggest
    },
    autoScroll (instance, topSize, aim, id) {
      instance.scrollTo(100, topSize, 300, function (scrollbar) {
        $('#' + id).find('.column_li').removeClass('active_filter')
        if (aim !== '') {
          $('#' + id).find('.' + aim).addClass('active_filter')
          instance.addListener(function (status) {
            // console.log(status)
          })
        }
      })
      // instance.stop()
    },
    linkFilterColumnAnimate (sourceId, targetId, callback) {
      $('#' + sourceId).find('.link_box').animate({
        'top': '0',
        'width': '0px',
        'height': '0',
        'left': '125px'
      })
      $('#' + targetId).find('.link_box').animate({
        'top': '0',
        'width': '0px',
        'height': '0',
        'left': '125px'
      }, 'fast', function () {
        callback()
      })
    },
    /*
    *  Table Func
    *  ==========================================================================
    */
    draggleTable: function (idList) {
      this.plumbInstance.draggable(idList)
    },
    /*
    *  Endpoint Func
    *  ==========================================================================
    */
    createEndpointConfig: function (newEndpointconfig, type) {
      return Object.assign({}, this.endpointConfig(type), newEndpointconfig)
    },
    removeAllEndpoints (plumb) {
      plumb.deleteEveryEndpoint()
    },
    addSelectPoints: function (guid, jsplumb, pointType, columnName, columnType, topPoint) {
      var anchor = [[1.0, 0.4, 1.5, 0], [0, 0.4, -1, 0]]
      var scope = 'link'
      if (topPoint) {
        anchor = [[0.5, 0, 0.6, 0]]
        scope = 'showlink'
      }
      jsplumb.addEndpoint(guid, {anchor: anchor}, this.createEndpointConfig({
        scope: scope,
        parameters: {
          data: {
            guid: guid,
            column: {
              columnName: columnName,
              columnType: columnType
            }}
        },
        uuid: guid + columnName
      }, pointType))
      // this.draggleTable([guid])
      this.refreshPlumbObj(jsplumb)
    },
    /*
    *  Connect Func
    *  ==========================================================================
    */
    connect: function (p1, p2, jsplumb, otherProper) {
      var defaultPata = {uuids: [p1, p2], editable: true}
      $.extend(defaultPata, otherProper)
      jsplumb.connect(defaultPata)
    },
    addShowLink: function (p1, p2, count) {
      this.connect(p1, p2, this.plumbInstance)
      // conn.getOverlay('label').setLabel(count)
    },
    refreshPlumbObj: function (plumb) {
      plumb = plumb || jsPlumb
      plumb.repaintEverything()
    }
  },
  mounted () {
    let _this = this
    jsPlumb.ready(() => {
      // _this.plumbInstance = jsPlumb.getInstance()

      _this.plumbInstance = jsPlumb.getInstance({
        DragOptions: { cursor: 'pointer', zIndex: 2000 },
        ConnectionOverlays: [
          [ 'Arrow', {
            location: 1,
            visible: true,
            width: 11,
            length: 11,
            id: 'ARROW',
            events: {
              click: function () { alert('you clicked on the arrow overlay') }
            }
          } ],
          [ 'Label', {
            location: 0.1,
            id: 'label',
            cssClass: 'aLabel',
            events: {
              tap: function () { alert('hey') }
            }
          }]
        ],
        Container: 'canvas'
      })
      var basicType = {
        connector: 'StateMachine',
        paintStyle: { stroke: 'red', strokeWidth: 4 },
        hoverPaintStyle: { stroke: 'blue' },
        overlays: [
          'Arrow'
        ]
      }
      _this.plumbInstance.registerConnectionType('basic', basicType)
      _this.draggleTable(_this.tableIdList)
      _this.plumbInstance.bind('connection', function (info, originalEvent) {
        console.log(info.sourceEndpoint.getUuid())
        if (info.connection.scope !== 'showlink') {
          _this.links[info.connection.id] = [info.sourceEndpoint.getParameters().data, info.targetEndpoint.getParameters().data]
          // _this.removeAllEndpoints(_this.plumbInstance)
          _this.plumbInstance.deleteEndpoint(info.sourceEndpoint.getUuid())
          _this.plumbInstance.deleteEndpoint(info.targetEndpoint.getUuid())

          _this.addSelectPoints(info.connection.sourceId, _this.plumbInstance, 'source', '', '', true)
          _this.addSelectPoints(info.connection.targetId, _this.plumbInstance, 'target', '', '', true)

          _this.linkFilterColumnAnimate(info.connection.sourceId, info.connection.targetId, function () {
            _this.cancelFilterColumn(info.connection.sourceId)
            _this.cancelFilterColumn(info.connection.targetId)
            _this.addShowLink(info.connection.sourceId, info.connection.targetId, _this.connectsCount)
          })
        }
      })
    })
    _this.plumbInstance.bind('connectionDrag', function (connection) {
      console.log('connection ' + connection.id + ' is being dragged. suspendedElement is ', connection.suspendedElement, ' of type ', connection.suspendedElementType)
    })

    _this.plumbInstance.bind('connectionDragStop', function (connection) {
      console.log('connection ' + connection.id + ' was dragged')
    })

    _this.plumbInstance.bind('connectionMoved', function (params) {
      console.log('connection ' + params.connection.id + ' was moved')
    })
    jsPlumb.fire('jsPlumbDemoLoaded', this.plumbInstance)
    Scrollbar.initAll()
  },
  computed: {
    tableIdList: function () {
      var tableIdListArr = []
      this.tableList.forEach(function (table) {
        tableIdListArr.push(table.guid)
      })
      return tableIdListArr
    },
    connectsCount: function () {
      var count = 0
      this.links.forEach(function () {
        count = count + 1
      })
      return count
    }
  }
}
</script>
<style lang="less">
   [data-scrollbar] .scrollbar-track-y, [scrollbar] .scrollbar-track-y, scrollbar .scrollbar-track-y{
     right: 4px;
   }
   .table_box{
       width: 250px;
       background-color: #64748a; 
       position: absolute;
       height: 422px;
       .link_box{
         i{
           padding: 5px;
           cursor: pointer;
         }
         position: absolute;
         background-color: #58b7ff;
         height: 40px;
         width: 100%;
         top:150px;
         color:#fff;
         line-height: 40px;
         // text-align: center;
         z-index: 11;
       }
       .tool_box{
        position: absolute;
        right: 6px;
        top:10px;
        font-size: 12px;
        color:#fff;
       }
       .more_tool{
          text-align: center;
          i{
            color: #fff;
            font-weight:lighter;
          }
       }
       .filter_box{
        padding: 10px;
       }
       .table_name{
         height: 36px;
         line-height: 36px;
         background-color: #8492a6;
         padding-left:10px;
         color:#fff;
       }
       .columns_box{
        height: 300px;
        overflow: hidden;
       }
       ul{
        // position: absolute;
        margin-top: 10px;
        li{
          list-style: none;
          font-size: 12px;
          height: 30px;
          line-height: 30px;
          color:#fff;
          span{
            display: inline-block;
          }
          &.active_filter{
            // color:red;
          }
          .kind{
            color: #20a0ff;
            width: 20px;
            height: 20px;
            text-align: center;
            line-height: 20px;
            margin-left: 5px;
            border-radius: 10px;
            cursor:pointer;
            &:hover{
              background-color:#59697f;
            }
          }
          .column_type{
            float: right;
            margin-right: 6px;
            color:#ccc;
          }
        }
       }
   }
</style>
