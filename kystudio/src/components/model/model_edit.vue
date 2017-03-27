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




      <el-dialog title="外键关系" v-model="dialogVisible" size="tiny">
        <span>
          
           <el-table
              :data="currentTableLinks"
              border
              style="width: 100%">
              <el-table-column
                label="主键"
                width="180">
                 <template scope="scope">
                  <el-popover trigger="hover" placement="top">
                    <p>{{ getTableInfoByGuid(scope.row[0]).name + '.' + scope.row[2] }}</p>
                    <div slot="reference" class="name-wrapper">
                      <el-tag type="success">{{ getTableInfoByGuid(scope.row[0]).name + '.' + scope.row[2] }}</el-tag>
                    </div>
                  </el-popover>
                </template>
              </el-table-column>
              <el-table-column
                label="关系"
                width="80">
                <template scope="scope">
                  <div slot="reference" class="name-wrapper">
                    <el-tag>{{ scope.row[4] }}</el-tag>
                  </div>
                </template>
              </el-table-column>
              <el-table-column
                label="外键"
                width="180">
                <template scope="scope">
                  <el-popover trigger="hover" placement="top">
                    <p>{{ getTableInfoByGuid(scope.row[1]).name + '.' + scope.row[3]}}</p>
                    <div slot="reference" class="name-wrapper">
                      <el-tag type="primary">{{ getTableInfoByGuid(scope.row[1]).name + '.' + scope.row[3]}}</el-tag>
                    </div>
                  </el-popover>
                </template>
              </el-table-column>
              
              <el-table-column label="操作" >
                <template scope="scope">
                  <confirm-btn v-on:okFunc='delConnect(scope.row)' :tips="deleteLinkTips"><el-button size="small"
          type="danger">删除</el-button></confirm-btn>
                </template>
              </el-table-column>
            </el-table>

        </span>
        <!-- <span slot="footer" class="dialog-footer">
          <el-button @click="dialogVisible = false">取 消</el-button>
          <el-button type="primary" @click="dialogVisible = false">确 定</el-button>
        </span> -->

      </el-dialog>

    </div>

</template>
<script>
import { jsPlumb } from 'jsplumb'
import { sampleGuid } from '../../util/index'

import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
export default {
  name: 'modeledit',
  data () {
    return {
      dialogVisible: false,
      deleteLinkTips: '你确认要删除该连接吗？',
      selectColumn: {},
      plumbInstance: null,
      plumbInstanceForShowLink: null,
      links: [],
      showLinkCons: {},
      currentTableLinks: [],
      pointType: 'source',
      tableList: [{
        'filterName': '',
        'name': 'User_ProfileUser_ProfileUser_ProfileUser_Profile',
        'database': 'default',
        'alias': 'User_Profile',
        'guid': '123',
        'columns': [{'id': '1', 'name': 'A', 'datatype': 'bigint'}, {'id': '2', 'name': 'B', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'TEST', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }, {
        'name': 'test2',
        'database': 'default',
        'alias': 'Product',
        'guid': '456',
        'columns': [{'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }, {
        'name': 'test22',
        'database': 'default',
        'alias': 'Product',
        'guid': sampleGuid(),
        'columns': [{'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }],
      endpointConfig: function (type, isShowLinkPoint) {
        var connectorPaintStyle = {
          strokeWidth: 2,
          stroke: '#61B7CF',
          joinstyle: 'round'
        }
    // .. and this is the hover style.
        var connectorHoverStyle = {
          strokeWidth: 3,
          stroke: '#216477'
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
          connector: [ 'Bezier', { curviness: -13 } ], // 设置连线为贝塞尔曲线
          // connector: [ 'Flowchart', { stub: [40, 60], gap: 10, cornerRadius: 5, alwaysRespectStubs: true } ],
          connectorStyle: connectorPaintStyle,
          hoverPaintStyle: endpointHoverStyle,
          connectorHoverStyle: connectorHoverStyle,
          dragOptions: {}
        }
    // the definition of target endpoints (will appear when the user drags a connection)
        var targetEndpoint = {
          endpoint: 'Dot',
          paintStyle: { fill: '#7AB02C', radius: 7 },
          hoverPaintStyle: endpointHoverStyle,
          maxConnections: -1,
          dropOptions: { hoverClass: 'hover', activeClass: 'active' },
          isTarget: true
        }
        console.log(isShowLinkPoint)
        if (type === 'source') {
          if (isShowLinkPoint) {
            sourceEndpoint.paintStyle.radius = 1
          }
          return sourceEndpoint
        } else {
          if (isShowLinkPoint) {
            targetEndpoint.paintStyle.radius = 1
          }
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
      this.pointType = this.pointType === 'source' ? 'target' : 'source'
      if (columnName) {
        this.$set(this.selectColumn, id, {columnName: columnName, columnType: columnType})
        this.removePoint(id + columnName)
        this.addSelectPoints(id, this.plumbInstance, this.pointType, columnName, columnType)
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
    getTableInfoByGuid: function (guid) {
      var len = this.tableList && this.tableList.length || 0
      for (var i = 0; i < len; i++) {
        if (this.tableList[i].guid === '' + guid) {
          return this.tableList[i]
        }
      }
    },
    /*
    *  Endpoint Func
    *  ==========================================================================
    */
    createEndpointConfig: function (newEndpointconfig, type, isShowLinkPoint) {
      return Object.assign({}, this.endpointConfig(type, isShowLinkPoint), newEndpointconfig)
    },
    removeAllEndpoints (plumb) {
      plumb.deleteEveryEndpoint()
    },
    addSelectPoints: function (guid, jsplumb, pointType, columnName, columnType, isShowLinkPoint) {
      var anchor = [[1.0, 0.4, 1.5, 0], [0, 0.4, -1, 0]]
      var scope = 'link'
      if (isShowLinkPoint) {
        anchor = [[0.5, 0, 0.6, 0], [0.5, 1, 0.6, 1], [0, 0.5, 0, 0.6], [1, 0.5, 1, 0.6]]
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
      }, pointType, isShowLinkPoint))
      this.draggleTable([guid])
      this.refreshPlumbObj(jsplumb)
    },
    removePoint: function (uuid) {
      this.plumbInstance.deleteEndpoint(uuid)
    },
    /*
    *  Connect Func
    *  ==========================================================================
    */
    connect: function (p1, p2, jsplumb, otherProper) {
      var _this = this
      var defaultPata = {uuids: [p1, p2],
        editable: true,
        overlays: ['Arrow', ['Label', {id: p1 + (p2 + 'label'),
          label: 'foo',
          location: 60,
          events: {
            tap: function () {
              _this.showLinkGrid(p1, p2)
            }
          } }]]}
      $.extend(defaultPata, otherProper)
      jsplumb.connect(defaultPata)
    },
    addConnect: function (p1, p2, col1, col2, type, con) {
      var links = this.links
      var hasSame = false
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === '' + p1 && links[i][1] === '' + p2 && links[i][2] === col1 && links[i][3] === col2) {
          links[i][4] = type
          links[i][5] = con
          hasSame = true
        }
      }
      if (!hasSame) {
        this.links.push([p1, p2, col1, col2, type, con])
      }
    },
    delConnect: function (connect) {
      var links = this.links
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === connect[0] && links[i][1] === connect[1] && links[i][2] === connect[2] && links[i][3] === connect[3]) {
          this.links.splice(i, 1)
          break
        }
      }
      this.getConnectsByTableIds(connect[0], connect[1])
      var showLinkCon = this.showLinkCons[connect[0] + '' + connect[1]]
      console.log(showLinkCon)
      if (this.currentTableLinks.length === 0) {
        delete this.showLinkCons[connect[0] + '' + connect[1]]
        this.plumbInstance.detach(showLinkCon)
      } else {
        this.setConnectLabelText(showLinkCon, connect[0], connect[1], '' + this.currentTableLinks.length)
      }
      console.log(this.links)
    },
    setConnectLabelText: function (conn, p1, p2, text) {
      conn.getOverlay(p1 + (p2 + 'label')).setLabel('' + text)
      this.showLinkCons[p1 + '' + p2] = conn
    },
    getConnectCountByTableIds: function (p1, p2) {
      var count = 0
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === '' + p1 && this.links[i][1] === '' + p2) {
          count = count + 1
        }
      }
      return count
    },
    getConnectsByTableIds: function (p1, p2) {
      this.currentTableLinks = []
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p1 && this.links[i][1] === p2) {
          this.currentTableLinks.push(this.links[i])
        }
      }
      console.log(444)
      console.log(this.currentTableLinks)
    },
    addShowLink: function (p1, p2) {
      this.connect(p1, p2, this.plumbInstance)
    },
    showLinkGrid: function (p1, p2) {
      this.dialogVisible = true
      this.getConnectsByTableIds(p1, p2)
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
          } ]],
        Container: 'model_edit'
      })
      // var basicType = {
      //   connector: 'StateMachine',
      //   paintStyle: { stroke: 'red', strokeWidth: 4 },
      //   hoverPaintStyle: { stroke: 'blue' },
      //   overlays: [
      //     'Arrow'
      //   ]
      // }
      _this.draggleTable(jsPlumb.getSelector('.table_box'))
      // _this.plumbInstance.draggable(jsPlumb.getSelector('.table_box'), { grid: [20, 20] })
      // _this.plumbInstance.registerConnectionType('basic', basicType)
      // // _this.draggleTable(_this.tableIdList, {
      // //   drag: function (e) {
      // //     return true
      // //   }
      // // })
      _this.plumbInstance.bind('connection', function (info, originalEvent) {
        if (info.connection.scope !== 'showlink') {
          _this.addConnect(info.connection.sourceId, info.connection.targetId, info.sourceEndpoint.getParameters().data.column.columnName, info.targetEndpoint.getParameters().data.column.columnName, 'left', info.connection)
          // _this.removeAllEndpoints(_this.plumbInstance)
          _this.removePoint(info.sourceEndpoint.getUuid())
          _this.removePoint(info.targetEndpoint.getUuid())
          var hisConn = _this.showLinkCons[info.connection.sourceId + '' + info.connection.targetId]
          console.log(hisConn)
          if (!hisConn) {
            console.log('start link')
            _this.addSelectPoints(info.connection.sourceId, _this.plumbInstance, 'source', '', '', true)
            _this.addSelectPoints(info.connection.targetId, _this.plumbInstance, 'target', '', '', true)
            _this.addShowLink(info.connection.sourceId, info.connection.targetId)
          }
          _this.linkFilterColumnAnimate(info.connection.sourceId, info.connection.targetId, function () {
            _this.cancelFilterColumn(info.connection.sourceId)
            _this.cancelFilterColumn(info.connection.targetId)
          })
        } else {
          _this.setConnectLabelText(info.connection, info.connection.sourceId, info.connection.targetId, _this.getConnectCountByTableIds(info.connection.sourceId, info.connection.targetId))
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
   #model_edit{
    position: relative;
    background-color: #475568;
    background-image: url('../../assets/img/jsplumb.png');
    background-repeat:repeat;
    width: 1000000px;
    height: 100000px;
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
          cursor: pointer;
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
   .jtk-overlay {
    background-color: white;
    padding: 0.4em;
    font: 12px sans-serif;
    color: #444;
    z-index: 21;
    border: 2px solid #f5a623;
    opacity: 0.8;
    cursor: pointer;
    width: 20px;
    height: 20px;
    border-radius: 10px;
    text-align: center;
    line-height: 20px;
}
</style>
