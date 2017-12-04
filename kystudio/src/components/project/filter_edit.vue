<!-- 暂不使用 -->
<template>
<div>
      <el-button class="ksd-mb-20" type="primary" @click="addFilter"> ＋ {{$t('filter')}}</el-button>
      <el-form :inline="true" label-width="160px" v-model="filterMeta" v-show="editFilterVisible" class="demo-form-inline">
        <el-form-item :label="$t('filterName')">
          <el-input   :placeholder="$t('filterName')" v-model="filterMeta.name"></el-input>
        </el-form-item>
       <el-form-item :label="$t('tableName')">
          <el-select  :placeholder="$t('tableName')" v-model="filterMeta.filter_table_type">
            <el-option label="HDFS" value="HDFS"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('resourcePath')">
           <el-input  :placeholder="$t('resourcePath')" v-model="filterMeta.filter_resource_identifier"></el-input>
        </el-form-item>
        <br/>
        <el-form-item :label="$t('filterDescription')">
           <el-input :rows="3" style="width:400px" type="textarea" :placeholder="$t('filterDescription')" v-model="filterMeta.description"></el-input>
        </el-form-item>
         <br/>
        <el-form-item class="ksd-fright">
           <el-button  @click="editFilterVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="saveFilter">{{$t('kylinLang.common.save')}}</el-button>
        </el-form-item>
      </el-form>

      <el-table
        :data="filterList"
        border
        style="width: 100%">
        <el-table-column
          prop="name"
          :label="$t('tableName')"
         >
        </el-table-column>

        <el-table-column
          prop="filter_resource_identifier"
          :label="$t('resourcePath')"
          >
        </el-table-column>
        <el-table-column
          prop="description"
          :label="$t('filterDescription')"
          >
        </el-table-column>
       <el-table-column v-if="hasActionAccess"
          :label="$t('kylinLang.common.action')"
          width="120">
          <template scope="scope" >
            <el-button
              @click="removeFilter(scope.row)"
              type="text"
              size="small">
              {{$t('kylinLang.common.delete')}}
            </el-button>
            <el-button
              @click="beginEdit(scope.row)"
              type="text"
              size="small">
              {{$t('kylinLang.common.edit')}}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, hasPermission, hasRole, kapConfirm } from '../../util/business'
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
          this.hasActionAccess = hasRole(this, 'ROLE_ADMIN') || hasPermission(this, 16)
        })
      })
    },
    beginEdit (row) {
      this.editFilterVisible = true
      this.filterMeta = Object.assign(this.filterMeta, row)
    },
    removeFilter (row) {
      kapConfirm(this.$t('kylinLang.common.confirmDel')).then(() => {
        this.delFilter({project: this.project, filterName: row.name}).then((res) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
          this.loadFilters()
        })
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            showClose: true,
            duration: 0
          })
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
        this.$message(this.$t('kylinLang.common.saveSuccess'))
        this.editFilterVisible = false
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            showClose: true,
            duration: 0
          })
        })
      })
    }
  },
  computed: {
  },
  created () {
    this.loadFilters()
  },
  locales: {
    'en': {filter: 'Filter', tableName: 'Table Name', filterName: 'Filter Name', resourcePath: 'Resource Path', filterDescription: 'Filter Description'},
    'zh-cn': {filter: '过滤器', tableName: '表名', filterName: '过滤器名称', resourcePath: '资源路径', filterDescription: '描述'}
  }
}
</script>
<style scoped="">
</style>
