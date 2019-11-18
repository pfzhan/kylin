<template>
  <div class="model-sql ksd-mb-15">
    <kap-editor ref="modelSql" :value="convertHiveSql"  height="390" lang="sql" theme="chrome" :readOnly="true" :dragable="false" :isAbridge="true">
    </kap-editor>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccess, handleError } from 'util/index'
import { mapActions, mapGetters } from 'vuex'
@Component({
  name: 'ModelSql',
  props: ['model'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getModelSql: 'GET_MODEL_SQL'
    })
  }
})
export default class modelSql extends Vue {
  convertHiveSql = ''
  mounted () {
    this.getModelSql({
      model: this.model,
      project: this.currentSelectedProject
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.convertHiveSql = data.replace(/`/g, '')
      })
    }, (res) => {
      handleError(res)
    })
  }
}
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .model-sql {
    .smyles_editor_wrap {
      overflow: hidden;
    }
  }
</style>
