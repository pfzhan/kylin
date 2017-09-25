<template>
  <div>
  <el-input v-if="getJSON !== ''"
    v-model="getJSON"
    type="textarea"
    :autosize="{ minRows: 4, maxRows: 10}"
    :readonly="true">
  </el-input>
  <el-card v-else>
    {{$t('NoSQLInfo')}}
  </el-card>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
export default {
  name: 'showsql',
  props: ['cube'],
  computed: {
    getJSON () {
      return this.cube.sql
    }
  },
  methods: {
    ...mapActions({
      getSql: 'GET_SAMPLE_SQL'
    }),
    loadCubeSql: function () {
      if (!this.cube.sql) {
        this.getSql(this.cube.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$set(this.cube, 'sql', data.sqls.join(';\r\n'))
          })
        }, (res) => {
          handleError(res)
        })
      }
    }
  },
  locales: {
    'en': {NoSQLInfo: 'No SQL Info.'},
    'zh-cn': {NoSQLInfo: '没有SQL的相关信息。'}
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
