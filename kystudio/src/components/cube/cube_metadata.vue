<template>
    <div class="paddingbox">   
      <editor v-model="json"  theme="chrome" class="ksd-mt-20" width="100%" height="400" ></editor>
      <el-button class="button_right" v-if="extraoption.type==='edit'" type="primary" @click="update">{{$t('save')}}</el-button>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import editor from 'vue2-ace-editor'
export default {
  name: 'cubeMetadata',
  props: ['extraoption'],
  data () {
    return {
      json: ''
    }
  },
  components: {
    editor
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      getScheduler: 'GET_SCHEDULER',
      loadRawTable: 'GET_RAW_TABLE',
      updateCube: 'UPDATE_CUBE'
    }),
    update: function () {
      let _this = this
      _this.$confirm('确认保存Cube？', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        _this.updateCube({project: _this.extraoption.project, cubeDescData: _this.json}).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            _this.$message({
              type: 'success',
              message: '保存成功!'
            })
          })
          _this.$emit('removetabs', 'edit' + _this.extraoption.cubeName)
        }).catch((res) => {
          handleError(res)
        })
      }).catch((e) => {
        console.log(e)
        _this.$message({
          type: 'info',
          message: '已取消保存'
        })
      })
    }
  },
  created () {
    if (this.extraoption.type === 'view') {
      this.json = JSON.stringify(this.extraoption.cubeDesc, 4, 4)
    } else {
      this.loadCubeDesc(this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data) => {
          this.json = JSON.stringify(data[0], 4, 4)
        })
      })
      this.loadRawTable(this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data) => {
          if (data) {
            this.usedRawTable = true
            this.$set(this.rawTable, 'tableDetail', data)
            this.initConvertedRawTable()
          }
        })
      })
      this.getScheduler(this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.initRepeatInterval(data)
          this.scheduler.desc.scheduled_run_time = data.scheduled_run_time
          this.scheduler.desc.partition_interval = data.partition_interval
        })
      })
    }
  },
  locales: {
    'en': {save: 'Save'},
    'zh-cn': {save: '保存'}
  }
}
</script>
<style scoped>
 .button_right {
  float: right;
  margin:20px 20px 20px 20px;

 }
</style>
