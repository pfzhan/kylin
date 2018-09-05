<template>
  <div>
    <el-dialog :title="$t('kylinLang.common.save')" :visible.sync="saveQueryFormVisible" width="780px" append-to-body  @close="closeSaveQueryDialog">
      <el-form :model="saveQueryMeta"  ref="saveQueryForm" :rules="rules" label-width="85px">
        <el-form-item :label="$t('kylinLang.query.querySql')" prop="sql">
          <kap_editor height="200" lang="sql" theme="chrome" v-model="saveQueryMeta.sql" dragbar="#393e53">
          </kap_editor>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.name')" prop="name">
          <el-input v-model="saveQueryMeta.name" size="medium" auto-complete="off" style="width: 50%"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.desc')" prop="description">
          <el-input v-model="saveQueryMeta.description" size="medium" type="textarea" style="width: 50%"></el-input>
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
import { mapActions } from 'vuex'
import { handleError } from '../../util/business'
export default {
  name: 'saveQueryDialog',
  props: ['show', 'extraoption'],
  data () {
    return {
      saveQueryFormVisible: false,
      rules: {
        name: [
          { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur' }
        ]
      },
      saveQueryMeta: {
        name: '',
        description: '',
        project: this.extraoption.project,
        sql: this.extraoption.sql
      }
    }
  },
  methods: {
    ...mapActions({
      saveQueryToServer: 'SAVE_QUERY'
    }),
    saveQuery () {
      this.$refs['saveQueryForm'].validate((valid) => {
        if (valid) {
          this.saveQueryToServer(this.saveQueryMeta).then((response) => {
            this.$message(this.$t('kylinLang.common.saveSuccess'))
            this.closeSaveQueryDialog()
            this.$emit('reloadSavedProject', 0)
          }, (res) => {
            handleError(res)
            this.closeSaveQueryDialog()
          })
        }
      })
    },
    closeSaveQueryDialog: function () {
      this.saveQueryFormVisible = false
      this.$emit('closeModal')
    }
  },
  watch: {
    'show' (v) {
      this.saveQueryFormVisible = v
    }
  }
}
</script>
