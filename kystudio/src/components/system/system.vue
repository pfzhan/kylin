<template>
  <div class="system-wrap" id="system">
    <el-row :gutter="20">
      <el-col :span="9">  
            <span class="server-type">{{$t('ServerConfig')}}</span>
            <el-button style="width: 28px;height:28px;" type="default" class="btn-refresh" @click="refreshConfig" size="mini"><icon name="refresh"></icon></el-button>
            <editor class="ksd-mt-4" ref="sysConfig" @init="editorInit" v-model="getServerConfig"  theme="chrome" width="100%" height="278" useWrapMode="true"></editor>
          <!-- <el-input class="textarea-wrap"
          type="textarea"
          :rows="18"
          :readonly="true"
          v-model="getServerConfig">
          </el-input> -->
      </el-col>
      <el-col :span="9">
            <span class="server-type">{{$t('ServerEnvironment')}}</span>
            <el-button style="width: 28px;height:28px;" type="default" class="btn-refresh" @click="refreshEnv" size="mini"><icon name="refresh"></icon></el-button>
            <editor class="ksd-mt-4" ref="envConfig" @init="editorInit" v-model="getServerEnvironment"  theme="chrome" width="100%" height="278"></editor>
         <!--  <el-input
          type="textarea"
          :rows="18"
          :readonly="true"
          v-model="getServerEnvironment">
          </el-input> -->
      </el-col>
      <el-col :span="6" class="action-wrap">
        <p style="font-size:13px;">{{$t('action')}}</p>
        <el-button style="margin-top: 22px;" type="primary" class="but-width bg_blue" @click="reload"><p class="p_font">{{$t('reloadMetadata')}}</p></el-button>
        <el-button class="but-width bg_blue" type="primary" @click="setConfig"><p class="p_font">{{$t('setConfig')}}</p></el-button>
        <el-button class="but-width bg_blue" type="primary" @click="backup">
          <p class="p_font">
            <!-- <icon name="cogs"></icon> -->
            {{$t('backup')}}
          </p>
        </el-button>
        <el-button class="but-width bg_blue" type="primary" @click="diagnosisSys" style="margin-bottom:30px;">
          <p class="p_font">
            <!-- <icon name="ambulance"></icon> -->
            {{$t('diagnosis')}}
          </p>
        </el-button>
      </el-col>
    </el-row>

    <el-dialog @close="closeSetConfig" :title="$t('setConfig')" v-model="setConfigFormVisible">
      <set_config  ref="setConfigForm" v-on:validSuccess="setConfigValidSuccess"></set_config>
      <div slot="footer" class="dialog-footer">
        <el-button @click="setConfigFormVisible = false">{{$t('cancel')}}</el-button>
        <el-button type="primary" @click="checkSetConfigForm">{{$t('yes')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog :title="$t('diagnosis')" v-model="diagnosisVisible">
      <diagnosis :selectTimer="true" :show="diagnosisVisible"></diagnosis>
    </el-dialog>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import setConfig from './set_config'
import diagnosis from './diagnosis'
import loginKybot from '../common/login_kybot.vue'
import startKybot from '../common/start_kybot.vue'
import protocolContent from '../system/protocol.vue'
export default {
  data () {
    return {
      activeName: 'system',
      setConfigFormVisible: false,
      diagnosisVisible: false,
      getServerEnvironment: '',
      getServerConfig: ''
    }
  },
  components: {
    'set_config': setConfig,
    'diagnosis': diagnosis,
    'login_kybot': loginKybot,
    'start_kybot': startKybot,
    'protocol_content': protocolContent
  },
  mounted () {
    var editor1 = this.$refs.envConfig.editor
    var editor2 = this.$refs.sysConfig.editor
    // editor1.setOption('gutters', 'huuuu')
    editor1.setOption('wrap', 'free')
    editor2.setOption('wrap', 'free')
    editor1.setReadOnly(true)
    editor2.setReadOnly(true)
  },
  methods: {
    ...mapActions({
      getEnv: 'GET_ENV',
      getConf: 'GET_CONF',
      reloadMetadata: 'RELOAD_METADATA',
      backupMetadata: 'BACKUP_METADATA',
      updateConfig: 'UPDATE_CONFIG',
      loadAllProjects: 'LOAD_ALL_PROJECT'
    }),
    reload: function () {
      this.$confirm(this.$t('reloadTip'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
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
    },
    editorInit () {
      // require('brace/mode/sql')
      // require('brace/theme/chrome')
    },
    setConfig: function () {
      this.setConfigFormVisible = true
    },
    closeSetConfig: function () {
      this.$refs['setConfigForm'].$refs['setConfigForm'].resetFields()
    },
    checkSetConfigForm: function () {
      this.$refs['setConfigForm'].$emit('setConfigFormValid')
    },
    setConfigValidSuccess: function (data) {
      this.updateConfig({key: data.key, value: data.value}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.refreshEnv()
          this.refreshConfig()
          this.$message({
            type: 'success',
            message: this.$t('setConfigSuccessful'),
            duration: 3000
          })
        })
        this.getConf()
      }).catch((res) => {
        handleError(res)
      })
      this.setConfigFormVisible = false
    },
    backup: function () {
      this.backupMetadata().then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('backupSuccess') + data
          })
        })
      }).catch((res) => {
        handleError(res)
      })
    },
    diagnosisSys: function () {
      this.diagnosisVisible = true
    },
    resetLoginKybotForm () {
      this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
    },
    closeLoginForm () {
      this.kyBotUploadVisible = false
      this.infoKybotVisible = true
    },
    closeStartLayer () {
      this.infoKybotVisible = false
    },
    refreshEnv: function () {
      this.getEnv().then((res) => {
        // handleSuccess(res, (data, code, status, msg) => {
        //   this.$notify({
        //     title: this.$t('success'),
        //     message: this.$t('successEnvironment'),
        //     type: 'success',
        //     duration: 3000
        //   })
        // })
        this.getServerEnvironment = JSON.stringify(this.$store.state.system.serverEnvironment).replace(/\\n/g, '\r').replace(/^"|"$/g, '')
      }, (res) => {
        handleError(res)
      })
    },
    refreshConfig: function () {
      let _this = this
      _this.getConf().then((result) => {
        // _this.$notify({
        //   title: _this.$t('success'),
        //   message: _this.$t('successConfig'),
        //   type: 'success',
        //   duration: 3000
        // })
        this.getServerConfig = JSON.stringify(this.$store.state.system.serverConfig).replace(/\\n/g, '\r').replace(/^"|"$/g, '')
      }).catch((res) => {
        handleError(res)
      })
    }
  },
  computed: {
  },
  created () {
    this.refreshEnv()
    this.refreshConfig()
  },
  locales: {
    'en': {ServerConfig: 'Server Config', ServerEnvironment: 'Server Environment', action: 'Actions', reloadMetadata: 'Reload Metadata', setConfig: 'Set Configuration', backup: 'Backup', diagnosis: 'Diagnosis', link: 'Links', success: 'Success', successEnvironment: 'Server environment get successfully', successConfig: 'Server config get successfully', reloadTip: 'Are you sure to reload metadata and clean cache? ', cancel: 'Cancel', yes: 'Yes', tip: 'Tip', reloadSuccessful: 'Reload metadata successful!', setConfigSuccessful: 'Set config successful!', autoUpload: 'KyBot Auto Upload', contentOne: 'By analyzing your diagnostic package, ', contentTwo: 'can provide online diagnostic, tuning and support service for KAP.', backupSuccess: 'Metadata backup successfully: '},
    'zh-cn': {ServerConfig: '服务器配置', ServerEnvironment: '服务器环境', action: '操作', reloadMetadata: '重载元数据', setConfig: '设置配置', backup: '备份', diagnosis: '诊断', link: '链接', success: '成功', successEnvironment: '成功获取环境信息', successConfig: '成功获取服务器配置', reloadTip: '确定要重载元数据并清理缓存? ', tip: '提示', cancel: '取消', yes: '确定', reloadSuccessful: '重载元数据成功!', setConfigSuccessful: '设置配置成功!', autoUpload: 'KyBot 自动上传', contentOne: '通过分析生成的诊断包，', contentTwo: '提供在线诊断，优化服务。', backupSuccess: '元数据备份成功: '}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  #system{
    margin-left: 30px;
    margin-right: 30px;
    .el-dialog__wrapper{
      overflow: visible;
    }
    .el-dialog--small{
      width: 50%;
    }
  }
  .system-wrap {
    .box-card{
    height: 500px;
  }
  .but-width{
    width: 100%;
    height: 55px;
    margin: 10px 0px 10px 0px;
  }
  .p_font {
    font-size:16px;
    color: #fff;
  }
  a {
    font-size:20px;
    margin: 30px 0px 10px 0px;
  }
  .bg_blue {
    background-color: #20a0ff;
    border: none!important;
  }
  .server-type {
    line-height: 36px;
    font-size: 13px;
    color: @fff;
  }
  .btn-refresh {
    margin-left:10px;
  }
  .el-textarea__inner {
    width: 95%;
    padding: 10px 15px;
    color: #888;
  }
  .action-wrap{
    .el-button{
      margin-left: 0;
    } 
    .blue {
      height: 40px;
      line-height: 40px;
      font-size: 18px;
      color: #20a0ff;
    }
    .blue:hover {
      text-decoration: none;
    }
  }
  .btn-agree {
    display: block;
    margin: 20px auto;
  }
  .agree-protocol {
    line-height:30px;
  }
}

</style>
