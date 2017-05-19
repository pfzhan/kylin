<template>
<div>
      <el-button class="ksd-mb-20" type="primary" @click="addFilter"> ＋ Filter</el-button>
      <el-form :inline="true" label-width="160px" v-model="filterMeta" v-show="editFilterVisible" class="demo-form-inline">
        <el-form-item label="Filter Name">
          <el-input   placeholder="filter name" v-model="filterMeta.name"></el-input>
        </el-form-item>
       <el-form-item label="Table Name">
          <el-select  placeholder="table name" v-model="filterMeta.filter_table_type">
            <el-option label="HDFS" value="HDFS"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Resource Path">
           <el-input  placeholder="Resource Path" v-model="filterMeta.filter_resource_identifier"></el-input>
        </el-form-item>
        <br/>
         <el-form-item label="Filter Description">
           <el-input :rows="3" style="width:400px" type="textarea" placeholder="Filter Description" v-model="filterMeta.description"></el-input>
        </el-form-item>
         <br/>
        <el-form-item class="ksd-fright">
           <el-button  @click="editFilterVisible = false">Cancel</el-button>
          <el-button type="primary" @click="saveFilter">Save</el-button>
        </el-form-item>
      </el-form>

      <el-table
        :data="filterList"
        border
        style="width: 100%">
        <el-table-column
          prop="name"
          label="Table Name"
         >
        </el-table-column>
        
        <el-table-column
          prop="filter_resource_identifier"
          label="Resource Path"
          >
        </el-table-column>
        <el-table-column
          prop="description"
          label="Description"
          >
        </el-table-column>
       <el-table-column v-if="hasActionAccess"
          label="操作"
          width="120">
          <template scope="scope" >
            <el-button
              @click="removeFilter(scope.row)"
              type="text"
              size="small">
              移除
            </el-button>
            <el-button
              @click="beginEdit(scope.row)"
              type="text"
              size="small">
              编辑
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, hasPermission, hasRole } from '../../util/business'
export default {
  name: 'cubelist',
  props: ['project', 'projectId'],
  data () {
    return {
      editFilterVisible: false,
      filterMeta: {
        'name': '',
        'filter_resource_identifier': '',
        'filter_table_type': 'HDFS',
        'description': ''
      },
      filterList: [],
      hasActionAccess: false
    }
  },
  methods: {
    ...mapActions({
      save: 'ADD_PROJECT_FILTER',
      uodateFilter: 'EDIT_PROJECT_FILTER',
      getFilters: 'GET_PROJECT_FILTER',
      delFilter: 'DEL_PROJECT_FILTER'
    }),
    initMeta () {
      this.filterMeta = {
        'name': '',
        'filter_resource_identifier': '',
        'filter_table_type': 'HDFS',
        'description': ''
      }
    },
    addFilter () {
      this.initMeta()
      this.editFilterVisible = true
    },
    loadFilters () {
      this.getFilters(this.project).then((res) => {
        handleSuccess(res, (data) => {
          this.filterList = data
          this.hasActionAccess = hasRole(this, 'ROLE_ADMIN') || hasPermission(this, this.projectId, 16)
        })
      })
    },
    beginEdit (row) {
      this.editFilterVisible = true
      this.filterMeta = Object.assign(this.filterMeta, row)
    },
    removeFilter (row) {
      this.$confirm('此操作将永久删除该Filter, 是否继续?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.delFilter({project: this.project, filterName: row.name}).then((res) => {
          this.$message({
            type: 'success',
            message: '删除成功!'
          })
          this.loadFilters()
        })
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message.error(msg)
        })
      })
    },
    saveFilter () {
      var action = 'save'
      if (this.filterMeta.uuid) {
        action = 'uodateFilter'
      }
      this[action]({
        extFilter: JSON.stringify(this.filterMeta),
        project: this.project
      }).then((res) => {
        this.loadFilters()
        this.$message('保存成功！')
        this.editFilterVisible = false
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message.error(msg)
        })
      })
    }
  },
  computed: {
  },
  created () {
    this.loadFilters()
  }
}
</script>
<style scoped="">
</style>
