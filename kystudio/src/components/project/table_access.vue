<template>
  <div class="table-access-block">
    <el-tabs activeName="tables" type="card" class="el-tabs--default">
      <el-tab-pane :label="$t('tables')" name="tables">
        <el-row>
          <el-col :span="8">
            <div class="access-card">
              <div class="access-title">{{$t('userOrGroupTitle')}} ({{users.length}})</div>
              <div class="access-search">
                <el-input size="small" :placeholder="$t('searchKey')" v-model="userFilter">
                  <i slot="prefix" class="el-input__icon el-icon-search"></i>
                </el-input>
              </div>
              <div class="access-content">
                <el-tree :data="usresdata" v-if="usresdata.length" show-overflow-tooltip :props="defaultProps" @node-click="handleNodeClick"></el-tree>
                <kap-nodata v-else>
                </kap-nodata>
              </div>
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>
      <el-tab-pane :label="$t('columns')" name="columns">
        <el-row>
          <el-col :span="8">
            <div class="access-card">
              <div class="access-title">{{$t('accessColumns')}} ({{columns.length}})</div>
              <div class="access-search">
                <el-input size="small" :placeholder="$t('searchKey')" v-model="columnFilter">
                  <i slot="prefix" class="el-input__icon el-icon-search"></i>
                </el-input>
              </div>
              <div class="access-content">
                <ul v-if="columns.length">
                  <li v-for="col in columns" :key="col">{{col}}</li>
                </ul>
                <kap-nodata v-else>
                </kap-nodata>
              </div>
            </div>
          </el-col>
          <el-col :span="8">
            <div class="access-card">
              <div class="access-title">{{$t('userOrGroupTitle')}} ({{users.length}})</div>
              <div class="access-search">
                <el-input size="small" :placeholder="$t('searchKey')" v-model="userFilter">
                  <i slot="prefix" class="el-input__icon el-icon-search"></i>
                </el-input>
              </div>
              <div class="access-content">
                <el-tree :data="usresdata" v-if="usresdata.length" show-overflow-tooltip :props="defaultProps" @node-click="handleNodeClick"></el-tree>
                <kap-nodata v-else>
                </kap-nodata>
              </div>
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>
      <el-tab-pane :label="$t('rows')" name="rows">
        <el-row>
          <el-col :span="16">
            <div class="access-card">
              <div class="access-title">{{$t('accessRows')}} ({{rows.length}})</div>
              <div class="access-search">
                <el-input size="small" :placeholder="$t('searchKey')" v-model="rowFilter">
                  <i slot="prefix" class="el-input__icon el-icon-search"></i>
                </el-input>
              </div>
              <div class="access-content">
                <ul v-if="rows.length">
                  <li v-for="row in rows" :key="row">{{row}}</li>
                </ul>
                <kap-nodata v-else>
                </kap-nodata>
              </div>
            </div>
          </el-col>
          <el-col :span="8">
            <div class="access-card">
              <div class="access-title">{{$t('userOrGroupTitle')}} ({{users.length}})</div>
              <div class="access-search">
                <el-input size="small" :placeholder="$t('searchKey')" v-model="userFilter">
                  <i slot="prefix" class="el-input__icon el-icon-search"></i>
                </el-input>
              </div>
              <div class="access-content">
                <el-tree :data="usresdata" v-if="usresdata.length" show-overflow-tooltip :props="defaultProps" @node-click="handleNodeClick"></el-tree>
                <kap-nodata v-else>
                </kap-nodata>
              </div>
            </div>
          </el-col>
        </el-row>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: ['users'],
  locales: {
    'en': {
      tables: 'Table',
      columns: 'Columns',
      rows: 'Rows',
      userOrGroupTitle: 'User/Group List',
      accessColumns: 'Column Access List',
      accessRows: 'Row Access List',
      searchKey: 'Please input a keyword'
    },
    'zh-cn': {
      tables: '表',
      columns: '列',
      rows: '行',
      userOrGroupTitle: '用户/组访问列表',
      accessColumns: '列级访问列表',
      accessRows: '行级访问列表',
      searchKey: '请输入关键字搜索'
    }
  }
})
export default class TableAccess extends Vue {
  activeName = 'tables'
  usresdata = []
  columns = []
  rows = []
  userFilter = ''
  columnFilter = ''
  rowFilter = ''
  defaultProps = {
    children: 'children',
    label: 'label'
  }
  handleNodeClick (data) {}
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
</style>
