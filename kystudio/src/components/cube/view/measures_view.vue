<template>
  <el-table
    :data="desc.measures"
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
      :label="$t('parameters')"
      width="500">
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
  </el-table>
</template>
<script>
import parameterTree from '../../common/parameter_tree'
export default {
  name: 'measures',
  props: ['desc'],
  components: {
    'parameter_tree': parameterTree
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', parameters: 'Parameters', datatype: 'Datatype', comment: 'Comment', returnType: 'Return Type'},
    'zh-cn': {name: '名称', expression: '表达式', parameters: '参数', datatype: '数据类型', comment: '注释', returnType: '返回类型'}
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
