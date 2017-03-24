<template>
<div>
  <el-table
    :data="cubeDesc.measures"
    style="width: 100%">
    <el-table-column
      property="name"
      :label="$t('name')">
    </el-table-column>
    <el-table-column
      property="function.expression"
      :label="$t('expression')">
    </el-table-column>    
    <el-table-column
      :label="$t('parameters')">
      <template scope="scope">
        <div class="paraTree">
          <ul >
            <parameter_tree :expression="scope.row.function.expression" :nextpara="scope.row.function.parameter" ></parameter_tree>
          </ul>
        </div>  
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('datatype')">      
    </el-table-column>  
    <el-table-column
      :label="$t('comment')">   
    </el-table-column>
    <el-table-column
      property="function.returntype"    
      :label="$t('returnType')">
    </el-table-column>  
    <el-table-column
      :label="$t('action')">
      <template scope="scope">
        <el-button type="success"  size="small" icon="edit" @click.native="editMeasure(scope.row)"></el-button>
        <el-button type="danger"  size="small" icon="delete"></el-button>
      </template>
    </el-table-column>                         
  </el-table>
  <el-button type="primary" @click.native="addMeasure">{{$t('addMeasure')}}</el-button>


  <el-dialog :title="$t('editMeasure')" v-model="FormVisible" top="5%" size="small">
    <add_measures  ref="measuresForm" :modelDesc="modelDesc" :measureDesc="measureDesc"></add_measures>
    <span slot="footer" class="dialog-footer">
      <el-button @click="FormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" >{{$t('yes')}}</el-button>
    </span>     
  </el-dialog>    
</div>  
</template>
<script>
import addMeasures from '../dialog/add_measures'
import parameterTree from '../../common/parameter_tree'
export default {
  name: 'measures',
  props: ['cubeDesc', 'modelDesc'],
  data () {
    return {
      FormVisible: false,
      measureDesc: {}
    }
  },
  methods: {
    addMeasure: function () {
      this.measureDesc = {
        name: ' ',
        function: {
          expression: 'SUM',
          parameter: {
            type: ' ',
            value: ' '
          },
          returntype: ''
        }
      }
      this.FormVisible = true
    },
    editMeasure: function (measure) {
      this.measureDesc = measure
      this.FormVisible = true
    }
  },
  components: {
    'parameter_tree': parameterTree,
    'add_measures': addMeasures
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', parameters: 'Parameters', datatype: 'Datatype', comment: 'Comment', returnType: 'Return Type', action: 'Action', addMeasure: 'Add Measure', editMeasure: 'Edit Measure', cancel: 'Cancel', yes: 'Yes'},
    'zh-cn': {name: '名称', expression: '表达式', parameters: '参数', datatype: '数据类型', comment: '注释', returnType: '返回类型', action: '操作', addMeasure: '添加度量', editMeasure: '编辑度量', cancel: '取消', yes: '确定'}
  }
}
</script>
<style scoped="">
.paraTree {
  padding: 3px;
  -webkit-border-radius: 4px;
  -moz-border-radius: 4px;
  border-radius: 4px;
  -webkit-box-shadow: inset 0 1px 1px rgba(0, 0, 0, 0.05);
  -moz-box-shadow: inset 0 1px 1px rgba(0, 0, 0, 0.05);
  box-shadow: inset 0 1px 1px rgba(0, 0, 0, 0.05)
}

.paraTree li {
  list-style-type: none;
  margin: 0;
  padding: 10px 5px 0 5px;
  position: relative
}

.paraTree li::before, .paraTree li::after {
  content: '';
  left: -20px;
  position: absolute;
  right: auto
}

.paraTree li::before {
  border-left: 1px solid #999;
  bottom: 50px;
  height: 100%;
  top: 0;
  width: 1px
}

.paraTree li::after {
  border-top: 1px solid #999;
  height: 20px;
  top: 25px;
  width: 25px
}

.paraTree li span {
  -moz-border-radius: 5px;
  -webkit-border-radius: 5px;
  border: 1px solid #999;
  border-radius: 5px;
  display: inline-block;
  padding: 3px 8px;
  text-decoration: none
}

.paraTree li.sub_li > span {
  cursor: pointer
}

.paraTree li:last-child::before {
  height: 30px
}

.paraTree li.sub_li > span:hover, .paraTree li.sub_li > span:hover + ul li span {
  background: #eee;
  border: 1px solid #94a0b4;
  color: #000
}
</style>
