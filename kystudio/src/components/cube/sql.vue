<template>
  <div>
    <editor v-if="cube.sql && cube.sql.length>0" ref="sqlPatterns" v-model="sqlPatterns"  theme="chrome" width="100%" useWrapMode="true" height="220" ></editor>
    <el-card v-else style="height:220;width: 90%">
      {{$t('NoSQLInfo')}}
    </el-card>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
export default {
  name: 'showSQL',
  props: ['cube'],
  computed: {
    sqlPatterns () {
      return this.cube.sql && this.cube.sql.join(';\r\n') || ''
    }
  },
  methods: {
    ...mapActions({
      getSql: 'GET_SAMPLE_SQL'
    }),
    loadCubeSql: function () {
      this.getSql(this.cube.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cube, 'sql', data.sqls)
          this.$nextTick(() => {
            if (this.cube.sql && this.cube.sql.length > 0) {
              var editor = this.$refs.sqlPatterns && this.$refs.sqlPatterns.editor || ''
              editor.setOption('wrap', 'free')
              editor.setReadOnly(true)
            }
          })
        })
      }, (res) => {
        handleError(res)
      })
    }
  },
  locales: {
    'en': {NoSQLInfo: 'No SQL patterns.'},
    'zh-cn': {NoSQLInfo: '没有"SQL查询记录"的相关信息。'}
  }
}
</script>
<style scoped="">
.box-card {
  width: 90%;
  .el-textarea{
    font-size:12px;
  }
}
</style>
