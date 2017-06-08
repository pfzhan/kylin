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
      if (!this.cube.sql) {
        this.getCubesSql(this.cube.name).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$set(this.cube, 'sql', data.sql)
          })
        }).catch((res) => {
          handleError(res)
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
