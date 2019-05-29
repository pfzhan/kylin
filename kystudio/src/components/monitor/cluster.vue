<template>
  <div class="cluster-detail">
    <p class="cluster-title ksd-mt-10">{{$t('serviceList')}}</p>
    <el-button class="ksd-mt-16" type="primary" plain size="medium" @click="refreshServiceList">
      {{$t('kylinLang.common.refresh')}}
    </el-button>
    <el-table class="ksd-mt-10"
      :data="serviceList"
      border
      :default-sort="{prop: 'type', order: 'ascending'}"
      max-height="242">
      <el-table-column
        prop="name"
        :label="$t('kylinLang.system.hostName')"
        align="left">
        <template slot-scope="scope">
          <p>{{scope.row.name}}</p>
        </template>
      </el-table-column>
      <el-table-column
        prop="type"
        :label="$t('kylinLang.system.type')"
        align="left"
        width="200">
        <template slot-scope="scope">
          {{serverType[scope.row.type]}}
        </template>
      </el-table-column>
      <el-table-column
        prop="status"
        :label="$t('kylinLang.system.status')"
        align="left"
        width="150">
        <template slot-scope="scope">
          <p>
            <span><i :class="[scope.row.status === 'Active' ? 'bg-active' : 'bg-unactive']" class="el-icon-ksd-good-health ksd-fs-20" style="cursor: default;"></i></span>
            {{scope.row.status}}
          </p>
        </template>
      </el-table-column>
      <el-table-column
        prop="action"
        :label="$t('kylinLang.common.action')"
        align="left"
        width="100">
        <template>
          <el-tooltip :content="$t('kylinLang.common.refresh')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_assign" @click="refreshServiceList">
            </i>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { indexOfObjWithSomeKey } from '../../util/index'
@Component({
  methods: {
    ...mapActions({
      getServiceState: 'GET_SERVICE_STATE'
    })
  },
  locales: {
    'en': {serviceList: 'Server List', reloadMetadata: 'Reload Metadata', backup: 'Backup Metadata', reloadCheck: 'Are you sure to reload metadata and clean cache? ', reloadSuccessful: 'Reload metadata successful.', backupSys: 'Are you sure to backup the system?', reloadTip: 'Reload the metadata of this instance.', backupTip: ' Backup the metadata of this instance.', workproperly: ' work properly.'},
    'zh-cn': {serviceList: '服务器列表', reloadMetadata: '重载元数据', setConfig: '设置配置', backup: '备份元数据', reloadCheck: '确定要重载元数据并清理缓存? ', reloadSuccessful: '重载元数据成功。', backupSys: '确定要进行系统备份? ', reloadTip: '重新载入全部元数据。', backupTip: '备份全部元数据。', workproperly: '工作正常'}
  }
})
export default class Cluster extends Vue {
  serverType = {
    qn: 'Query Node',
    jn1: 'Job Node (Leader)',
    jn2: 'Job Node (Follower)'
  }

  refreshServiceList () {
    this.getServiceState()
  }
  get serviceList () {
    var list = []
    if (Object.keys(this.$store.state.system.serviceState).length > 0) {
      this.$store.state.system.serviceState.jobNodes.forEach((node) => {
        list.push({
          name: node,
          type: 'jn2',
          status: 'Avaliable'
        })
      })
      this.$store.state.system.serviceState.selectedLeaders.forEach((node) => {
        var index = indexOfObjWithSomeKey(list, 'name', node)
        if (index >= 0) {
          list[index].type = 'jn1'
          list[index].status = 'Active'
        }
      })
      this.$store.state.system.serviceState.queryNodes.forEach((node) => {
        list.push({
          name: node,
          type: 'qn',
          status: 'Active'
        })
      })
    }
    return list
  }
  created () {
    this.refreshServiceList()
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .cluster-detail {
    margin: 0 20px;
    .cluster-title {
      font-size: 16px;
      color: @000;
      font-weight: @font-medium;
      line-height: 24px;
    }
    .bg-active {
      color: @normal-color-1;
    }
    .bg-unactive {
      color: @info-color-1;
    }
  }
</style>
