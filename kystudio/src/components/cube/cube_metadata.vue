<template>
    <div class="paddingbox">   
      <editor v-model="json"  theme="chrome" class="ksd-mt-20" width="100%" height="400" ></editor>
      <el-button class="button_right" v-if="extraoption.type==='edit'" type="primary" @click="update">{{$t('save')}}</el-button>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm } from '../../util/business'
export default {
  name: 'cubeMetadata',
  props: ['extraoption'],
  data () {
    return {
      json: '',
      saveData: {
        project: this.extraoption.project
      }
    }
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      getScheduler: 'GET_SCHEDULER',
      loadRawTable: 'GET_RAW_TABLE',
      updateCube: 'UPDATE_CUBE',
      draftCube: 'DRAFT_CUBE'
    }),
    update: function () {
      kapConfirm('确认保存Cube？').then(() => {
        var action = 'draftCube'
        if (!JSON.parse(this.json).is_draft) {
          action = 'updateCube'
        }
        console.log(action)
        this.saveData.cubeDescData = this.json
        this[action](this.saveData).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: '保存成功!'
            })
          })
          this.$emit('removetabs', 'edit' + this.extraoption.cubeName)
        }, (res) => {
          handleError(res)
        })
      }).catch((e) => {
        console.log(e)
        this.$message({
          type: 'info',
          message: e || '已取消保存'
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
          this.json = JSON.stringify(data.cube || data.draft, 4, 4)
        })
      })
      this.loadRawTable(this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data) => {
          if (data) {
            this.saveData.rawTableDescData = JSON.stringify(data.rawTable || data.draft)
          }
        })
      })
      this.getScheduler(this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          if (data) {
            this.saveData.schedulerJobData = JSON.stringify(data.rawTable || data.draft)
          }
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
