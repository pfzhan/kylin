<template>
  <div class="admin-detail">
    <p class="admin-title ksd-mt-10">元数据管理</p>
    <el-tooltip :content="$t('reloadTip')" class="ksd-mt-16" effect="dark" placement="top">
      <el-button type="primary" plain size="medium" @click="reload" icon="el-icon-ksd-load">
        {{$t('reloadMetadata')}}
      </el-button>
    </el-tooltip>

    <el-tooltip :content="$t('backupTip')" class="ksd-mt-16" effect="dark" placement="top">
      <div slot="content">
       {{$t('backupTip')}}
      </div>
      <el-button type="primary" plain size="medium" @click="backup" :loading="btnload" icon="el-icon-ksd-backup">
          {{$t('backup')}}
      </el-button>
    </el-tooltip>
    <el-table class="ksd-mt-10"
      :data="metadataReport"
      border>
    <el-table-column type="expand" min-width="30">
      <template slot-scope="props">
        <div v-if = "props.row && props.row.data.length > 0"> 
            <table class="ksd-table ksd-mt-10" v-for="item in props.row.data" :key="item.nodeName">
              <tr class="ksd-tr">
                <th>Error Node</th>
                <td>{{item.nodeName}}</td>
              </tr>
              <tr class="ksd-tr">
                <th>More Messages</th>
                <td>{{item.statusMsg}}</td>
              </tr>
            </table>
        </div>
            <table class="ksd-table ksd-mt-10" v-else>
              <tr class="ksd-tr">
                <th>Error Node</th>
                <td>No Data</td>
              </tr>
              <tr class="ksd-tr">
                <th>More Messages</th>
                <td>No Data</td>
              </tr>
            </table>
      </template>
    </el-table-column>
      <el-table-column
        prop="name"
        label="task"
        align="left">
      </el-table-column>
      <el-table-column
        prop="type"
        :label="$t('kylinLang.system.status')"
        width="100">
        <template slot-scope="scope">
          <i :class="[scope.row && scope.row.good ? 'el-icon-success ky-success' : scope.row && scope.row.crash ? 'el-icon-error ky-error' :'el-icon-warning ky-warning']">
          </i>
        </template>
      </el-table-column>
      <el-table-column
        prop="checkTime"
        label="checkTime"
        width="300">
      </el-table-column>
      <el-table-column
        prop="action"
        :label="$t('kylinLang.common.action')"
        width="100">
        <template>
          <el-tooltip :content="$t('kylinLang.common.refresh')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_assign" @click="refreshServiceList">
            </i>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>

    <p class="admin-title ksd-mt-40">其他管理</p>
    <el-row :gutter="20" class='others-manage'>
      <el-col :span="6">
        <div class="manage-status">
          <i :class="renderReportData['Zookeeper availability'] && renderReportData['Zookeeper availability'].good ? 'el-icon-success ky-success' : renderReportData['Zookeeper availability'] && renderReportData['Zookeeper availability'].crash ? 'el-icon-error ky-error' :'el-icon-warning ky-warning'"></i>
          <p class="ksd-mt-8">good health</p>
          <img class="availability-manage" src="../../assets/img/canary_Hive.png"/>
        </div>
        <p class="manage-title">Hive 连通性检测</p>
        <p class="manage-checktime">上测检测时间 {{renderReportData['Zookeeper availability'] && renderReportData['Zookeeper availability'].checkTime}}</p>
        <el-button size="medium" type="primary" plain>查看详情</el-button>
        <el-button size="medium" type="primary" plain>立即检测</el-button>
      </el-col>
      <el-col :span="6">
        <div class="manage-status">
          <i :class="renderReportData['Zookeeper availability'] && renderReportData['Zookeeper availability'].good ? 'el-icon-success ky-success' : renderReportData['Zookeeper availability'] && renderReportData['Zookeeper availability'].crash ? 'el-icon-error ky-error' :'el-icon-warning ky-warning'"></i>
          <p class="ksd-mt-8">good health</p>
          <img class="availability-manage" src="../../assets/img/zooKeeper.png"/>
        </div>
        <p class="manage-title">ZooKeeper 活性检测</p>
        <p class="manage-checktime">上测检测时间 {{renderReportData['Zookeeper availability'] && renderReportData['Zookeeper availability'].checkTime}}</p>
        <el-button size="medium" type="primary" plain>查看详情</el-button>
        <el-button size="medium" type="primary" plain>立即检测</el-button>
      </el-col>
      <el-col :span="6">
        <el-progress type="circle" :percentage="65" :stroke-width="12" :width="220">
        </el-progress>
        <div class="manage-file">
          <p >500.27 GB</p>
          <p class="ksd-mt-10">22327772222 Files</p>
        </div>
        <p class="manage-title">kyligence Storage 垃圾检测</p>
        <p class="manage-checktime">上测检测时间 {{renderReportData['Garbage cleanup'] && renderReportData['Garbage cleanup'].checkTime}}</p>
        <el-button size="medium" type="primary" plain>刷新</el-button>
        <el-button size="medium" type="primary" plain>立即清理</el-button>
      </el-col>
      <el-col :span="6">
        <el-progress type="circle" :percentage="25" :stroke-width="12" :width="220">
        </el-progress> 
        <div class="manage-file">
          <p >5.27 GB</p>
          <p class="ksd-mt-10">2222 Files</p>
        </div>
        <p class="manage-title">元数据垃圾检测</p>
        <p class="manage-checktime">上测检测时间 {{renderReportData['Garbage cleanup'] && renderReportData['Garbage cleanup'].checkTime}}</p>
        <el-button size="medium" type="primary" plain>刷新</el-button>
        <el-button size="medium" type="primary" plain>立即清理</el-button>
      </el-col>
    </el-row>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm, transToGmtTimeAfterAjax } from '../../util/business'
import { indexOfObjWithSomeKey } from '../../util/index'
@Component({
  methods: {
    ...mapActions({
      getCanaryReport: 'GET_CANARY_REPORT',
      getServiceState: 'GET_SERVICE_STATE',
      reloadMetadata: 'RELOAD_METADATA',
      backupMetadata: 'BACKUP_METADATA',
      loadAllProjects: 'LOAD_ALL_PROJECT'
    })
  },
  locales: {
    'en': {serviceList: 'Server List', reloadMetadata: 'Reload Metadata', backup: 'Backup Metadata', reloadCheck: 'Are you sure to reload metadata and clean cache? ', reloadSuccessful: 'Reload metadata successful.', backupSys: 'Are you sure to backup the system?', reloadTip: 'Reload the metadata of this instance.', backupTip: ' Backup the metadata of this instance.', workproperly: ' work properly.'},
    'zh-cn': {serviceList: '服务器列表', reloadMetadata: '重载元数据', setConfig: '设置配置', backup: '备份元数据', reloadCheck: '确定要重载元数据并清理缓存? ', reloadSuccessful: '重载元数据成功。', backupSys: '确定要进行系统备份? ', reloadTip: '重新载入全部元数据。', backupTip: '备份全部元数据。', workproperly: '工作正常'}
  }
})
export default class Admin extends Vue {
  btnload = false
  serverType = {
    qn: 'Query Node',
    jn1: 'Job Node (Leader)',
    jn2: 'Job Node (Follower)'
  }

  reload () {
    this.$confirm(this.$t('reloadCheck'), this.$t('kylinLang.common.notice'), {
      confirmButtonText: this.$t('kylinLang.common.ok'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      type: 'warning'
    }).then(() => {
      this.reloadMetadata().then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('reloadSuccessful')
          })
        })
        setTimeout(() => {
          this.loadAllProjects()
        }, 1000)
      }).catch((res) => {
        handleError(res)
      })
    }).catch(() => {
    })
  }
  backup () {
    kapConfirm(this.$t('backupSys')).then(() => {
      this.btnload = true
      this.backupMetadata().then((res) => {
        this.btnload = false
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.backupSuccessTip') + data,
            showClose: true,
            duration: 0
          })
        })
      }).catch((res) => {
        this.btnload = false
        handleError(res)
      })
    })
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
  get renderReportData () {
    let result = {}
    let timeZone = localStorage.getItem('GlobalSeverTimeZone') ? localStorage.getItem('GlobalSeverTimeZone') : ''
    for (let nodeName in this.$store.state.system.canaryReport) {
      let cur = this.$store.state.system.canaryReport[nodeName]
      if (cur) {
        cur.forEach((list) => {
          result[list.canaryName] = result[list.canaryName] || {good: false, error: false, crash: false, data: [], name: list.canaryName, lastCheckTime: list.lastCheckTime, checkTime: transToGmtTimeAfterAjax(list.lastCheckTime, timeZone, this)}
          if (list.status !== 'GOOD') {
            result[list.canaryName].data.push({nodeName: nodeName, status: list.status, statusMsg: list.statusMsg})
          }
          if (list.status === 'GOOD') {
            result[list.canaryName].good = true
          }
          if (list.status === 'ERROR' || list.status === 'CRASH') {
            result[list.canaryName].crash = true
          }
          if (list.status === 'WARNING') {
            result[list.canaryName].error = true
          }
          if (result[list.canaryName].lastCheckTime < list.lastCheckTime) {
            result[list.canaryName].lastCheckTime = list.lastCheckTime
            result[list.canaryName].checkTime = transToGmtTimeAfterAjax(list.lastCheckTime, timeZone, this)
          }
        })
      }
    }
    return result
  }
  get metadataReport () {
    let result = []
    result.push(this.renderReportData['Metadata store availability'])
    result.push(this.renderReportData['Metadata integrity'])
    result.push(this.renderReportData['Metadata synchronization'])
    return result
  }
  created () {
    this.refreshServiceList()
    this.getCanaryReport({local: false})
  }

  @Watch('$store.state.system.lang')
  onLangChange (val) {
    Vue.http.headers.common['Accept-Language'] = val === 'zh-cn' ? 'cn' : 'en'
    this.getCanaryReport({local: false})
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .admin-detail {
    margin: 0 20px;
    .admin-title {
      font-size: 16px;
      color: @000;
      font-weight: @font-medium;
      line-height: 24px;
    }
    .el-table__expanded-cell{
      padding: 20px;
    }
    .others-manage {
      margin: 26px 0px 100px;
      .el-col {
        text-align: center;
        position: relative;
        .manage-status {
          border: 4px solid @modeledit-bg-color;
          border-radius: 100%;
          height: 215px;
          width: 215px;
          margin: auto;
          p {
            color: @text-title-color
          }
          i {
            margin-top: 58px;
            font-size: 60px;
          }
          .availability-manage {
            width: 67px;
            height: 67px;
            margin: 27px auto 0px;
          }
        }
        .el-progress__text {
          font-size: 30px!important;
          color: @text-title-color;
          font-weight: @font-medium;
          top: 40%;
        }
        .manage-file {
          position: absolute;
          top: 37%;
          left: 50%;
          width: 220px;
          p {
            position: relative;
            left: -110px;
            color: @text-title-color;
          } 
        }
        .manage-title {
          font-size: 16px;
          font-weight: @font-medium;
          color: @text-title-color;
          margin-top: 14px;
        }
        .manage-checktime {
          font-size: 12px;
          color: @text-normal-color;
          margin-top: 7px;
          margin-bottom: 20px;
        }
      }
    }
  }
</style>
