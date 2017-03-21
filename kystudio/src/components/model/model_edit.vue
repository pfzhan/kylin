<template>
    <div id="model_edit">
      <div class="table_box" v-for="table in tableList" :id="table.guid" >
        <span class="tool_box"><i class="el-icon-setting"></i></span>
        <p class="table_name">{{table.alias||table.name}}</p>
        <p class="filter_box"><el-input v-model="input" size="small" placeholder="enter filter..."></el-input></p>
        <div class="columns_box">
          <ul>
          <li v-for="column in table.columns"><span class="kind">D</span><span class="column">{{column.name}}</span><span class="column_type">{{column.datatype}}</span></style></li>
        </ul>
        </div>
        <div class="more_tool"><i class="el-icon-d-caret"></i></div>
      </div>
    </div>
</template>
<script>
import { jsPlumb } from 'jsplumb'
import { sampleGuid } from '../../util/index'
export default {
  data () {
    return {
      tableList: [{
        'name': 'test1',
        'database': 'default',
        'alias': 'User_Profile',
        'guid': sampleGuid(),
        'columns': [{'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '1', 'name': 'LEAF_CATEG_ID', 'datatype': 'bigint'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}, {'id': '2', 'name': 'LEAF_CATEG_NAME', 'datatype': 'varchar(256)'}]
      }]
    }
  },
  methods: {
  },
  created () {
    console.log(jsPlumb)
    sampleGuid()
  }
}
</script>
<style lang="less">
   .table_box{
       width: 220px;
       background-color: #64748a; 
       position: absolute;
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
        height: 200px;
        overflow-y: auto;
       }
       ul{
        height: 100%;
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
