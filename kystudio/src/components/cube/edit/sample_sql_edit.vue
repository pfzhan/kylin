<template>
    <div class="table_margin">   
      <editor v-model="sampleSql.sqlString"  theme="chrome" class="ksd-mt-20" width="100%" height="400" ></editor>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
import editor from 'vue2-ace-editor'
export default {
  name: 'sampleSQL',
  props: ['sampleSql', 'cubeDesc'],
  components: {
    editor
  },
  methods: {
    ...mapActions({
      getSampleSql: 'GET_SAMPLE_SQL'
    })
  },
  created () {
    if (this.sampleSql.sqlString === '') {
      this.getSampleSql(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.sampleSql.sqlString = data.join('\r\n')
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
  },
  locales: {
    'en': {},
    'zh-cn': {}
  }
}
</script>
<style scoped>
 .table_margin {
   margin-top: 20px;
   margin-bottom: 20px;
 }
</style>
