<template>
  <div>
    <el-dialog :title="$t('kylinLang.common.save')" :visible.sync="saveQueryFormVisible" width="660px" append-to-body  @close="closeSaveQueryDialog">
      <el-form :model="saveQueryMeta" label-position="top" ref="saveQueryForm" :rules="rules" label-width="85px">
        <el-form-item :label="$t('kylinLang.query.querySql')" prop="sql">
          <kap_editor height="100" lang="sql" theme="chrome" v-model="saveQueryMeta.sql" dragbar="#393e53">
          </kap_editor>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.name')" prop="name">
          <el-input v-model="saveQueryMeta.name" size="medium" auto-complete="off"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.desc')" prop="description">
          <el-input v-model="saveQueryMeta.description" size="medium" type="textarea"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button size="medium" @click="closeSaveQueryDialog">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain size="medium" @click="saveQuery">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { handleError } from '../../util/business'
@Component({
  props: ['show', 'extraoption'],
  methods: {
    ...mapActions({
      saveQueryToServer: 'SAVE_QUERY'
    })
  }
})
export default class saveQueryDialog extends Vue {
  saveQueryFormVisible = false
  rules = {
    name: [
      { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur' }
    ]
  }
  saveQueryMeta = {
    name: '',
    description: '',
    project: this.extraoption.project,
    sql: this.extraoption.sql
  }

  saveQuery () {
    this.$refs['saveQueryForm'].validate((valid) => {
      if (valid) {
        this.saveQueryToServer(this.saveQueryMeta).then((response) => {
          this.$message({type: 'success', message: this.$t('kylinLang.common.saveSuccess')})
          this.closeSaveQueryDialog()
          this.$emit('reloadSavedProject', 0)
        }, (res) => {
          handleError(res)
          this.closeSaveQueryDialog()
        })
      }
    })
  }
  closeSaveQueryDialog () {
    this.saveQueryFormVisible = false
    this.$emit('closeModal')
  }

  @Watch('show')
  onShowChange (v) {
    this.saveQueryFormVisible = v
    this.saveQueryMeta.project = this.extraoption.project
    this.saveQueryMeta.sql = this.extraoption.sql
  }
}
</script>
