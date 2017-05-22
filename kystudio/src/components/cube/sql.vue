<template>
<div>
<el-input
  v-model="getJSON"
  type="textarea"
  :rows="18"
  :readonly="true">
</el-input>
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
      getCubesSql: 'GET_CUBE_SQL'
    }),
    loadCubeSql: function () {
      let _this = this
      if (!_this.cube.sql) {
        this.getCubesSql(_this.cube.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            _this.$set(_this.cube, 'sql', data.sql)
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 3000
            })
            // if (status === 404) {
            //   _this.$router.replace('access/login')
            // }
          })
        })
      }
    }
  }
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
