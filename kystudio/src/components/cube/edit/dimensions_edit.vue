<template>
<div>  
  <el-row :gutter="20">
    <el-col :span="6">
      <el-button type="primary" icon="plus" @click.native="addDimensions">{{$t('addDimensions')}}</el-button>
    </el-col>
    <el-col :span="6" :offset="12">
      <el-input :placeholder="$t('filter')" icon="search" v-model="input" :on-icon-click="handleIconClick"></el-input>
    </el-col>
  </el-row>


  <el-table
    :data="cubeDesc.dimensions"
    style="width: 100%">
    <el-table-column
      type="index">
    </el-table-column>    
    <el-table-column
      property="name"
      :label="$t('name')">
    </el-table-column>
    <el-table-column
      :label="$t('type')">
      <template scope="scope">
        <el-tag type="primary" v-if='scope.row.derived'>normal</el-tag>
        <el-tag type="primary" v-else>derived</el-tag>
      </template>      
    </el-table-column>    
    <el-table-column
      property="table"
      :label="$t('tableAlias')">
    </el-table-column>
    <el-table-column
      :label="$t('column')">
      <template scope="scope">
        <span v-if='scope.row.derived'>{{scope.row.derived}}</span>
        <span v-else>{{scope.row.column}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('datatype')">
    </el-table-column>
    <el-table-column
      :label="$t('cardinality')">
    </el-table-column>
    <el-table-column
      :label="$t('comment')">
    </el-table-column>  
    <el-table-column
      :label="$t('action')">
      <template scope="scope">
        <el-button type="success"  size="small" icon="edit"></el-button>
        <el-button type="danger"  size="small" icon="delete"></el-button>
      </template>
    </el-table-column>               
  </el-table>

  <el-dialog :title="$t('addDimensions')" v-model="FormVisible" top="5%" size="large">
    <add_dimensions  ref="dimensionsForm" :modelDesc="modelDesc" :cubeDimensions="cubeDesc.dimensions"></add_dimensions>
    <span slot="footer" class="dialog-footer">
      <el-button @click="FormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" >{{$t('yes')}}</el-button>
    </span>     
  </el-dialog>  
</div>  
</template>
<script>
import addDimensions from '../dialog/add_dimensions'
export default {
  name: 'dimensions',
  props: ['cubeDesc', 'modelDesc'],
  data () {
    return {
      input: '',
      FormVisible: false,
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    addDimensions: function () {
      this.FormVisible = true
    },
    handleIconClick: function () {
      console.log(this.modelDesc)
    }
  },
  components: {
    'add_dimensions': addDimensions
  },
  locales: {
    'en': {name: 'Name', type: 'Type', tableAlias: 'Table Alias', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment', action: '操作', addDimensions: 'Add Dimensions', filter: 'Filter...', cancel: 'Cancel', yes: 'Yes'},
    'zh-cn': {name: '名称', type: '类型', tableAlias: '表别名', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释', action: '操作', addDimensions: '添加维度', filter: '过滤器', cancel: '取消', yes: '确定'}
  }
}
</script>
<style scoped="">

</style>
