<template>
  <el-form
    ref="form"
    class="source-csv"
    label-position="top"
    size="medium"
    :model="form"
    :rules="rules">
    <el-row :gutter="10">
      <el-col :span="24">
        <el-form-item :label="$t('pleaseInputSql')" prop="ddl">
          <kap-editor ref="csvSql" height="170" lang="sql" theme="chrome"  v-model="form.ddl"></kap-editor>
          <el-button class="ksd-mt-10" :loading="verifyLoading" @click="verify" :disabled="!form.ddl">{{$t('verify')}}</el-button>
          <template v-if="verifyResult.result">
            <div class="verify-result-box error-box" v-if="verifyResult.result === 'fail'">
              <div class="label">{{$t('errorInfo')}}</div>
              <div class="ksd-mt-4" v-html="filterInjectScript(verifyResult.verifyFailMsg).replace(/\n/g, '<br/>')"></div>
            </div>
            <div v-else class="verify-success">
              <i class='el-icon-success ksd-mr-4'></i>{{this.$t('checkSuccess')}}
            </div>
          </template>
        </el-form-item>
      </el-col>
    </el-row>
  </el-form>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'

import { mapActions } from 'vuex'
import locales from './locales'
import { handleSuccess, handleError, filterInjectScript } from '../../../../../util'

@Component({
  methods: {
    ...mapActions({
      verifyCSVSql: 'VERIFY_CSV_SQL'
    })
  },
  props: {
    isEditable: {
      type: Boolean,
      default: true
    },
    editType: {
      type: String
    },
    form: {
      type: Object,
      default: () => ({})
    }
  },
  locales
})
export default class SourceHiveSetting extends Vue {
  get rules () {
    return {
      ddl: [{
        required: true, message: this.$t('pleaseInputSql'), trigger: 'change'
      }]
    }
  }
  @Watch('form.ddl')
  watchDDL () {
    this.$refs.form.validateField('ddl')
  }
  filterInjectScript = filterInjectScript
  verifyLoading = false
  verifyResult = {
    verifyFailMsg: '',
    result: ''
  }
  verify () {
    this.verifyLoading = true
    this.verifyResult.result = ''
    this.verifyCSVSql({
      ddl: this.form.ddl
    }).then((res) => {
      this.verifyLoading = false
      handleSuccess(res, (data, code, msg) => {
        if (!data) {
          this.verifyResult.verifyFailMsg = msg
        }
        this.verifyResult.result = data ? 'success' : 'fail'
      })
    }, (res) => {
      this.verifyLoading = false
      handleError(res)
    })
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.source-csv {
  .verify-result-box {
    .label {
      color:@error-color-1
    }
    line-height:1;
    border: 1px solid #cfd8dc;
    border-radius: 2px;
    font-size: 12px;
    margin-top: 15px;
    padding: 10px;
    box-sizing: border-box;
    overflow-y: auto;  
  }
  .verify-success {
    i {
      color:@color-success;
    }
  }
}
</style>
