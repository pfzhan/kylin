<template>
  <el-input v-if="cubeDesc.desc.sampleSql"
      type="textarea"
      :rows="12"
      v-model="cubeDesc.desc.sampleSql"
      :readonly="true">
  </el-input>
  <p v-else>{{$t('noSampleSql')}}</p>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
export default {
  name: 'sampleSql',
  props: ['cubeDesc'],
  methods: {
    ...mapActions({
      getSampleSql: 'GET_SAMPLE_SQL'
    })
  },
  created () {
    let _this = this
    if (_this.cubeDesc.desc.sampleSql) {
      _this.getSampleSql(_this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.$set(_this.cubeDesc.desc, 'sampleSql', data.join('\r\n'))
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          console.log(status, 30000)
          if (status === 404) {
            // _this.$router.replace('access/login')
          }
        })
      })
    }
  },
  locales: {
    'en': {noSampleSql: 'No Sample SQL Information'},
    'zh-cn': {noSampleSql: '无样例查询信息'}
  }
}
</script>
<style scoped="">
.box-card{
  height: 500px;
}
.but-width{
  width: 100%;
  height: 70px;
  margin: 10px 0px 10px 0px;
}
p {
  font-size:30px;
}
a {
  font-size:20px;
  margin: 30px 0px 10px 0px;
}
</style>
