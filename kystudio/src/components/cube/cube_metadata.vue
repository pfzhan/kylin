<template>
    <div>   
      <editor v-model="json"  theme="chrome" width="100%" height="400" ></editor>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
export default {
  name: 'cubeMetadata',
  props: ['cubeDesc'],
  data () {
    return {
      json: '',
      saveData: {
        project: this.cubeDesc.project
      }
    }
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      getScheduler: 'GET_SCHEDULER',
      loadRawTable: 'GET_RAW_TABLE'
    })
  },
  created () {
    var commonPara = {cubeName: this.cubeDesc.name, project: this.cubeDesc.project}
    this.loadCubeDesc(commonPara).then((res) => {
      handleSuccess(res, (data) => {
        this.json = JSON.stringify(data.cube || data.draft, 4, 4)
      })
    }, (res) => {
      handleError(res)
    })
    this.loadRawTable(commonPara).then((res) => { // 为了保存时保留原值
      handleSuccess(res, (data) => {
        if (data) {
          this.saveData.rawTableDescData = JSON.stringify(data.rawTable || data.draft)
        }
      })
    })
    this.getScheduler(commonPara).then((res) => {  // 为了保存时保留原值
      handleSuccess(res, (data, code, status, msg) => {
        if (data) {
          this.saveData.schedulerJobData = JSON.stringify(data.rawTable || data.draft)
        }
      })
    })
    this.$on('descFormValid', (t) => {
      let action = 'draftCube'
      if (!JSON.parse(this.json).is_draft) {
        action = 'updateCube'
      }
      let jsonParseData = JSON.parse(this.json)
      if (jsonParseData && (+jsonParseData.engine_type === 100 || +jsonParseData.engine_type === 99)) {
        delete jsonParseData.hbase_mapping
      }
      this.saveData.cubeDescData = JSON.stringify(jsonParseData)
      this.$emit('validSuccess', {json: this.saveData, type: action})
    })
  }
}
</script>
<style lang="less">
</style>
