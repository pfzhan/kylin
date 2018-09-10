<template>
  <div id="queryHistory">
    <query_history_table :queryHistoryData="queryHistoryData" v-on:openAgg="openAgg"></query_history_table>
    <kap-pager ref="queryHistoryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="queryHistoryData.length"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      title="Aggregate Index"
      :visible.sync="aggDetailVisible"
      class="agg-dialog"
      width="1104px">
      <el-row :gutter="20">
        <el-col :span="16">
          <div class="cubois-chart-block">
            <div class="ksd-mt-10 ksd-mr-10 ksd-fright agg-amount-block">
              <span>Aggregate Amount</span>
              <el-input v-model.trim="aggAmount" size="small"></el-input>
            </div>
            <div id="visualization"></div>
          </div>
        </el-col>
        <el-col :span="8">
          <el-card class="agg-detail-card">
            <div slot="header" class="clearfix">
              <span>Aggregate Detail</span>
            </div>
            <div class="detail-content">
              <el-row :gutter="5"><el-col :span="11" class="label">ID:</el-col><el-col :span="13">{{aggDetail.id}}</el-col></el-row>
              <el-row :gutter="5">
                <el-col :span="11" class="label">Dimension and Order:</el-col>
                <el-col :span="13"><div v-for="item in aggDetail.dim" :key="item" class="dim-item">{{item}}</div></el-col>
              </el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Data Size:</el-col><el-col :span="13">{{aggDetail.dataSize}}</el-col></el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Data Range:</el-col><el-col :span="13">{{aggDetail.dateFrom | gmtTime}} To {{aggDetail.dateTo | gmtTime}}</el-col></el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Served Query amount:</el-col><el-col :span="13">{{aggDetail.amount}} Query</el-col></el-row>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync } from '../../util/index'
import queryHistoryTable from './query_history_table'
import { CodeFlower } from 'util/code_flower'
@Component({
  methods: {
    ...mapActions({
      getHistoryList: 'GET_HISTORY_LIST'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'query_history_table': queryHistoryTable
  }
})
export default class QueryHistory extends Vue {
  aggDetailVisible = false
  queryCurrentPage = 1
  queryHistoryData = [
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'ACCELERATING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'PARTLY_ACCELERATED', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'PARTLY_ACCELERATED', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'PARTLY_ACCELERATED', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'FULLY_ACCELERATED', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false}
  ]
  aggAmount = 1205
  aggDetail = {
    id: '2234kdrkg343532342jk',
    dim: ['Dimension_1', 'Dimension_2', 'Sum ( Price )', 'Dimension_5', 'TopN ( Seller_ID ) group by Sum ( Price )'],
    dataSize: '256MB',
    dateFrom: 1524829437628,
    dateTo: 1524829437628,
    amount: 12
  }
  /* eslint-disable */
  flowerJson = {
    "name": "flare",
    "size": 2025,
    "children": [
    {
     "name": "analytics",
     "size": 250,
     "children": [
      {
       "name": "cluster",
       "size": 250,
       "children": [
        {"name": "AgglomerativeCluster", "size": 3938},
        {"name": "CommunityStructure", "size": 3812},
        {"name": "HierarchicalCluster", "size": 6714},
        {"name": "MergeEdge", "size": 743}
       ]
      },
      {
       "name": "graph",
       "size": 250,
       "children": [
        {"name": "BetweennessCentrality", "size": 3534},
        {"name": "LinkDistance", "size": 5731},
        {"name": "MaxFlowMinCut", "size": 7840},
        {"name": "ShortestPaths", "size": 5914},
        {"name": "SpanningTree", "size": 3416}
       ]
      },
      {
       "name": "optimization",
       "size": 250,
       "children": [
        {"name": "AspectRatioBanker", "size": 7074}
       ]
      }
     ]
    },
    {
     "name": "animate",
     "size": 250,
     "children": [
      {"name": "Easing", "size": 17010},
      {"name": "FunctionSequence", "size": 5842},
      {
       "name": "interpolate",
       "size": 250,
       "children": [
        {"name": "ArrayInterpolator", "size": 1983},
        {"name": "ColorInterpolator", "size": 2047},
        {"name": "DateInterpolator", "size": 1375},
        {"name": "Interpolator", "size": 8746},
        {"name": "MatrixInterpolator", "size": 2202},
        {"name": "NumberInterpolator", "size": 1382},
        {"name": "ObjectInterpolator", "size": 1629},
        {"name": "PointInterpolator", "size": 1675},
        {"name": "RectangleInterpolator", "size": 2042}
       ]
      },
      {"name": "ISchedulable", "size": 1041},
      {"name": "Parallel", "size": 5176},
      {"name": "Pause", "size": 449},
      {"name": "Scheduler", "size": 5593},
      {"name": "Sequence", "size": 5534},
      {"name": "Transition", "size": 9201},
      {"name": "Transitioner", "size": 19975},
      {"name": "TransitionEvent", "size": 1116},
      {"name": "Tween", "size": 6006}
     ]
    },
    {
     "name": "data",
     "size": 250,
     "children": [
      {
       "name": "converters",
       "size": 250,
       "children": [
        {"name": "Converters", "size": 721},
        {"name": "DelimitedTextConverter", "size": 4294},
        {"name": "GraphMLConverter", "size": 9800},
        {"name": "IDataConverter", "size": 1314},
        {"name": "JSONConverter", "size": 2220}
       ]
      },
      {"name": "DataField", "size": 1759},
      {"name": "DataSchema", "size": 2165},
      {"name": "DataSet", "size": 586},
      {"name": "DataSource", "size": 3331},
      {"name": "DataTable", "size": 772},
      {"name": "DataUtil", "size": 3322}
     ]
    },
    {
     "name": "display",
     "size": 250,
     "children": [
      {"name": "DirtySprite", "size": 8833},
      {"name": "LineSprite", "size": 1732},
      {"name": "RectSprite", "size": 3623},
      {"name": "TextSprite", "size": 10066}
     ]
    },
    {
     "name": "flex",
     "size": 250,
     "children": [
      {"name": "FlareVis", "size": 4116}
     ]
    },
    {
     "name": "physics",
     "size": 250,
     "children": [
      {"name": "DragForce", "size": 1082},
      {"name": "GravityForce", "size": 1336},
      {"name": "IForce", "size": 319},
      {"name": "NBodyForce", "size": 10498},
      {"name": "Particle", "size": 2822},
      {"name": "Simulation", "size": 9983},
      {"name": "Spring", "size": 2213},
      {"name": "SpringForce", "size": 1681}
     ]
    },
    {
     "name": "query",
     "size": 250,
     "children": [
      {"name": "AggregateExpression", "size": 1616},
      {"name": "And", "size": 1027},
      {"name": "Arithmetic", "size": 3891},
      {"name": "Average", "size": 891},
      {"name": "BinaryExpression", "size": 2893},
      {"name": "Comparison", "size": 5103},
      {"name": "CompositeExpression", "size": 3677},
      {"name": "Count", "size": 781},
      {"name": "DateUtil", "size": 4141},
      {"name": "Distinct", "size": 933},
      {"name": "Expression", "size": 5130},
      {"name": "ExpressionIterator", "size": 3617},
      {"name": "Fn", "size": 3240},
      {"name": "If", "size": 2732},
      {"name": "IsA", "size": 2039},
      {"name": "Literal", "size": 1214},
      {"name": "Match", "size": 3748},
      {"name": "Maximum", "size": 843},
      {
       "name": "methods",
       "size": 250,
       "children": [
        {"name": "add", "size": 593},
        {"name": "and", "size": 330},
        {"name": "average", "size": 287},
        {"name": "count", "size": 277},
        {"name": "distinct", "size": 292},
        {"name": "div", "size": 595},
        {"name": "eq", "size": 594},
        {"name": "fn", "size": 460},
        {"name": "gt", "size": 603},
        {"name": "gte", "size": 625},
        {"name": "iff", "size": 748},
        {"name": "isa", "size": 461},
        {"name": "lt", "size": 597},
        {"name": "lte", "size": 619},
        {"name": "max", "size": 283},
        {"name": "min", "size": 283},
        {"name": "mod", "size": 591},
        {"name": "mul", "size": 603},
        {"name": "neq", "size": 599},
        {"name": "not", "size": 386},
        {"name": "or", "size": 323},
        {"name": "orderby", "size": 307},
        {"name": "range", "size": 772},
        {"name": "select", "size": 296},
        {"name": "stddev", "size": 363},
        {"name": "sub", "size": 600},
        {"name": "sum", "size": 280},
        {"name": "update", "size": 307},
        {"name": "variance", "size": 335},
        {"name": "where", "size": 299},
        {"name": "xor", "size": 354},
        {"name": "_", "size": 264}
       ]
      },
      {"name": "Minimum", "size": 843},
      {"name": "Not", "size": 1554},
      {"name": "Or", "size": 970},
      {"name": "Query", "size": 13896},
      {"name": "Range", "size": 1594},
      {"name": "StringUtil", "size": 4130},
      {"name": "Sum", "size": 791},
      {"name": "Variable", "size": 1124},
      {"name": "Variance", "size": 1876},
      {"name": "Xor", "size": 1101}
     ]
    },
    {
     "name": "scale",
     "size": 250,
     "children": [
      {"name": "IScaleMap", "size": 2105},
      {"name": "LinearScale", "size": 1316},
      {"name": "LogScale", "size": 3151},
      {"name": "OrdinalScale", "size": 3770},
      {"name": "QuantileScale", "size": 2435},
      {"name": "QuantitativeScale", "size": 4839},
      {"name": "RootScale", "size": 1756},
      {"name": "Scale", "size": 4268},
      {"name": "ScaleType", "size": 1821},
      {"name": "TimeScale", "size": 5833}
     ]
    },
    {
     "name": "util",
     "size": 250,
     "children": [
      {"name": "Arrays", "size": 8258},
      {"name": "Colors", "size": 10001},
      {"name": "Dates", "size": 8217},
      {"name": "Displays", "size": 12555},
      {"name": "Filter", "size": 2324},
      {"name": "Geometry", "size": 10993},
      {
       "name": "heap",
       "size": 250,
       "children": [
        {"name": "FibonacciHeap", "size": 9354},
        {"name": "HeapNode", "size": 1233}
       ]
      },
      {"name": "IEvaluable", "size": 335},
      {"name": "IPredicate", "size": 383},
      {"name": "IValueProxy", "size": 874},
      {
       "name": "math",
       "size": 250,
       "children": [
        {"name": "DenseMatrix", "size": 3165},
        {"name": "IMatrix", "size": 2815},
        {"name": "SparseMatrix", "size": 3366}
       ]
      },
      {"name": "Maths", "size": 17705},
      {"name": "Orientation", "size": 1486},
      {
       "name": "palette",
       "size": 250,
       "children": [
        {"name": "ColorPalette", "size": 6367},
        {"name": "Palette", "size": 1229},
        {"name": "ShapePalette", "size": 2059},
        {"name": "SizePalette", "size": 2291}
       ]
      },
      {"name": "Property", "size": 5559},
      {"name": "Shapes", "size": 19118},
      {"name": "Sort", "size": 6887},
      {"name": "Stats", "size": 6557},
      {"name": "Strings", "size": 22026}
     ]
    },
    {
     "name": "vis",
     "size": 250,
     "children": [
      {
       "name": "axis",
       "size": 250,
       "children": [
        {"name": "Axes", "size": 1302},
        {"name": "Axis", "size": 24593},
        {"name": "AxisGridLine", "size": 652},
        {"name": "AxisLabel", "size": 636},
        {"name": "CartesianAxes", "size": 6703}
       ]
      },
      {
       "name": "controls",
       "size": 250,
       "children": [
        {"name": "AnchorControl", "size": 2138},
        {"name": "ClickControl", "size": 3824},
        {"name": "Control", "size": 1353},
        {"name": "ControlList", "size": 4665},
        {"name": "DragControl", "size": 2649},
        {"name": "ExpandControl", "size": 2832},
        {"name": "HoverControl", "size": 4896},
        {"name": "IControl", "size": 763},
        {"name": "PanZoomControl", "size": 5222},
        {"name": "SelectionControl", "size": 7862},
        {"name": "TooltipControl", "size": 8435}
       ]
      },
      {
       "name": "data",
       "size": 250,
       "children": [
        {"name": "Data", "size": 20544},
        {"name": "DataList", "size": 19788},
        {"name": "DataSprite", "size": 10349},
        {"name": "EdgeSprite", "size": 3301},
        {"name": "NodeSprite", "size": 19382},
        {
         "name": "render",
         "size": 250,
         "children": [
          {"name": "ArrowType", "size": 698},
          {"name": "EdgeRenderer", "size": 5569},
          {"name": "IRenderer", "size": 353},
          {"name": "ShapeRenderer", "size": 2247}
         ]
        },
        {"name": "ScaleBinding", "size": 11275},
        {"name": "Tree", "size": 7147},
        {"name": "TreeBuilder", "size": 9930}
       ]
      },
      {
       "name": "events",
       "size": 250,
       "children": [
        {"name": "DataEvent", "size": 2313},
        {"name": "SelectionEvent", "size": 1880},
        {"name": "TooltipEvent", "size": 1701},
        {"name": "VisualizationEvent", "size": 1117}
       ]
      },
      {
       "name": "legend",
       "size": 250,
       "children": [
        {"name": "Legend", "size": 20859},
        {"name": "LegendItem", "size": 4614},
        {"name": "LegendRange", "size": 10530}
       ]
      },
      {
       "name": "operator",
       "size": 250,
       "children": [
        {
         "name": "distortion",
         "size": 250,
         "children": [
          {"name": "BifocalDistortion", "size": 4461},
          {"name": "Distortion", "size": 6314},
          {"name": "FisheyeDistortion", "size": 3444}
         ]
        },
        {
         "name": "encoder",
         "size": 250,
         "children": [
          {"name": "ColorEncoder", "size": 3179},
          {"name": "Encoder", "size": 4060},
          {"name": "PropertyEncoder", "size": 4138},
          {"name": "ShapeEncoder", "size": 1690},
          {"name": "SizeEncoder", "size": 1830}
         ]
        },
        {
         "name": "filter",
         "size": 250,
         "children": [
          {"name": "FisheyeTreeFilter", "size": 5219},
          {"name": "GraphDistanceFilter", "size": 3165},
          {"name": "VisibilityFilter", "size": 3509}
         ]
        },
        {"name": "IOperator", "size": 1286},
        {
         "name": "label",
         "size": 250,
         "children": [
          {"name": "Labeler", "size": 9956},
          {"name": "RadialLabeler", "size": 3899},
          {"name": "StackedAreaLabeler", "size": 3202}
         ]
        },
        {
         "name": "layout",
         "size": 250,
         "children": [
          {"name": "AxisLayout", "size": 6725},
          {"name": "BundledEdgeRouter", "size": 3727},
          {"name": "CircleLayout", "size": 9317},
          {"name": "CirclePackingLayout", "size": 12003},
          {"name": "DendrogramLayout", "size": 4853},
          {"name": "ForceDirectedLayout", "size": 8411},
          {"name": "IcicleTreeLayout", "size": 4864},
          {"name": "IndentedTreeLayout", "size": 3174},
          {"name": "Layout", "size": 7881},
          {"name": "NodeLinkTreeLayout", "size": 12870},
          {"name": "PieLayout", "size": 2728},
          {"name": "RadialTreeLayout", "size": 12348},
          {"name": "RandomLayout", "size": 870},
          {"name": "StackedAreaLayout", "size": 9121},
          {"name": "TreeMapLayout", "size": 9191}
         ]
        },
        {"name": "Operator", "size": 2490},
        {"name": "OperatorList", "size": 5248},
        {"name": "OperatorSequence", "size": 4190},
        {"name": "OperatorSwitch", "size": 2581},
        {"name": "SortOperator", "size": 2023}
       ]
      },
      {"name": "Visualization", "size": 16540}
     ]
    }
    ]
  }
  
  openAgg () {
    this.aggDetailVisible = true
    setTimeout(() => {
      const myFlower = new CodeFlower('#visualization', 700, 600)
      myFlower.update(this.flowerJson)
    }, 0)
  }

  async loadHistoryList (pageIndex, pageSize) {
    const res = await this.getHistoryList({
      project: this.currentSelectedProject || null,
      limit: pageSize || 10,
      offset: pageIndex || 0
    })
    const data = await handleSuccessAsync(res)
    this.queryHistoryData = data.query_histories
  }

  created () {
    this.loadHistoryList()
  }

  pageCurrentChange (offset, pageSize) {
    this.queryCurrentPage = offset + 1
    this.loadHistoryList(offset, pageSize)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryHistory {
    padding: 0 20px 50px 20px;
    .agg-dialog {
      #visualization {
        height: 600px;
        width: 100%;
        .node {
          cursor: pointer;
          stroke: #3182bd;
          stroke-width: 1.5px;
        }
        .link {
          fill: none;
          stroke: #9ecae1;
          stroke-width: 1.5px;
        }
        .el-tooltip__popper {
          padding: 5px;
          background: @text-normal-color;
          color: #fff;
        }
      }
      .agg-amount-block {
        .el-input {
          width: 120px;
        }
      }
      .cubois-chart-block {
        border: 1px solid @line-border-color;
        height: 638px;
      }
      .agg-detail-card {
        height: 638px;
        box-shadow: none;
        .el-card__header {
          background-color: @grey-3;
          color: @text-title-color;
          font-size: 16px;
        }
        .el-card__body {
          padding: 10px;
          .detail-content {
            .el-row {
              margin-bottom: 10px;
              .dim-item {
                margin-bottom: 5px;
              }
            }
          }
        }
        .label {
          text-align: right;
        }
      }
    }
  }
</style>
