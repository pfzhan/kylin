<template>
  <div>
    <el-input
      class="model-json"
      :value="JSON.stringify(jsonInfo, '', 4)"
      type="textarea"
      :rows="18"
      :readonly="true">
    </el-input></div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccess, handleError } from 'util/index'
import { mapActions, mapGetters } from 'vuex'
@Component({
  props: ['model'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getModelJson: 'GET_MODEL_JSON'
    })
  }
})
export default class modelJSON extends Vue {
  jsonInfo = null
  mounted () {
    this.getModelJson({
      model: this.model,
      project: this.currentSelectedProject
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.jsonInfo = JSON.parse(data)
      })
    }, (res) => {
      handleError(res)
    })
  }
}
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .model-json {
    margin: 20px 0;
    .el-textarea__inner:focus {
      border-color: @line-border-color;
    }
  }
</style>
