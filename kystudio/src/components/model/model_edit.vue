<template>
    <div id="model_edit">
      <div class="table_box" v-for="table in tableList" :id="table.guid" >
        <span class="tool_box"><i class="el-icon-setting"></i></span>
        <p class="table_name">{{table.alias||table.name}}</p>
        <p class="filter_box"><el-input v-model="table.filterName" v-on:change="filterColumn(table.filterName,table.guid)"  size="small" placeholder="enter filter..."></el-input></p>
        <section data-scrollbar class="columns_box">
          <ul>
            <li v-for="column in table.columns" :key="column.guid" :class="column.name" class="column_li"><span class="kind">D</span><span class="column">{{column.name}}</span><span class="column_type">{{column.datatype}}</span></style></li>
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
      plumbInstance: null,
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
        'columns': [{'id': '1', 'name': 'LEAF_CATEG_ID1', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }]
    }
  },
  beforeDestroy () {
    this.plumbInstance.deleteEveryEndpoint()
  },
  methods: {
    filterColumn: function (filter, id) {
      // console.log(filter, id)
      var instance = Scrollbar.get($('#' + id).find('.columns_box')[0])
      console.log(instance, document.getElementById(id))
      var suggestObj = this.getColumnByLikeFilter(id, filter)
      // console.log(this.getColumnByLikeFilter(id, filter))
      // document.querySelector('.' + this.getColumnByLikeFilter(id, filter)).scrollIntoView()
      this.autoScroll(instance, suggestObj.index * 30, suggestObj.className, id)
    },
    getColumnByLikeFilter: function (guid, filter) {
      var suggest = {
        className: '',
        index: 0
      }
      if (filter === '') {
        return suggest
      }
      this.tableList.forEach(function (table) {
        if (table.guid === guid) {
          table.columns.forEach(function (col, index) {
            if (col.name.toUpperCase().indexOf(filter.toUpperCase()) >= 0) {
              suggest.className = col.name
              suggest.index = index
              return suggest
            }
          })
        }
      })
      return suggest
    },
    draggleTable: function (jsPlumb, idList) {
      jsPlumb.draggable(idList)
    },
    autoScroll (instance, topSize, aim, id) {
      instance.scrollTo(100, topSize, 300, function (scrollbar) {
        $('#' + id).find('.' + aim).css('color', 'red')
        $('#' + id)
      })
    },
    createPointConfig: function (newLinkConfig) {
      let linkConfig = {
        // 设置连接点的形状为圆形
        endpoint: 'Rectangle',
        // 设置连接点的颜色
        paintStyle: { fill: '#46b8da', width: 10, height: 10 },
        // 是否可以拖动（作为连线起点）
        isSource: true,
        // 连接点的标识符，只有标识符相同的连接点才能连接
        scope: 'green dot',
        // 设置连线为贝塞尔曲线
        connector: [ 'Bezier', { curviness: 163 } ],
        // 设置连接点最多可以连接几条线
        maxConnections: 100,
        // 是否可以放置（作为连线终点）
        isTarget: true,
        connectorStyle: {
          strokeWidth: 5,
          stroke: '#66a8fa'
        },
        dropOptions: {
        // 释放时指定鼠标停留在该元素上使用的css class
          hoverClass: 'dropHover',
        // 设置放置相关的css
          activeClass: 'dragActive'
        },
        beforeDetach: function (conn) { // 绑定一个函数，在连线前弹出确认框
         // delete that.connects[info.connection.id];
        },
        reattachConnections: function () {
        // alert(1);
        },
        onMaxConnections: function (info) { // 绑定一个函数，当到达最大连接个数时弹出提示框
        // alert("Cannot drop connection " + info.connection.id + " : maxConnections has been reached on Endpoint " + info.endpoint.id);
        },
        onConnectionDetached: function () {
        // alert(2);
        },
        beforeDrop: function () {
        // that.beginDrag=true;
          return true
        }
      }
      linkConfig = Object.assign({}, linkConfig, newLinkConfig)
      return linkConfig
    },
    addPoints: function (jsPlumb) {
      let _this = this
      this.tableList.forEach(function (table) {
        table.columns.forEach(function (columns, i) {
          var h = (106 / 400 + i * 30 / 400) > 1 ? 1 : (106 / 400 + i * 30 / 400)
          jsPlumb.addEndpoint(table.guid, {anchor: [[1.0, h, 1.5, 0], [0, h, -1, 0]]}, _this.createPointConfig())
        })
      })
    }
  },
  mounted () {
    let _this = this
    jsPlumb.ready(() => {
      _this.plumbInstance = jsPlumb.getInstance()
      _this.draggleTable(_this.plumbInstance, _this.tableIdList)
      _this.addPoints(_this.plumbInstance)
    })
    // var el = document.querySelector('.columns_box')
    Scrollbar.initAll()
    console.log($)
  },
  computed: {
    tableIdList: function () {
      var tableIdListArr = []
      this.tableList.forEach(function (table) {
        tableIdListArr.push(table.guid)
      })
      return tableIdListArr
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
        overflow-y: auto;
       }
       ul{
        // position: absolute;
        margin-top: 10px;
        li{
          list-style: none;
          font-size: 12px;
          height: 30px;
          color:#fff;
          span{
            display: inline-block;
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
