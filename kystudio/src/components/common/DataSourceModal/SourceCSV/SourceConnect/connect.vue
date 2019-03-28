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
        <el-form-item :label="$t('url')" prop="url">
          <el-input
            @change="autoFulfillType"
            v-model="form.url"
            :placeholder="$t('kylinLang.common.pleaseInput')">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <!-- <el-row :gutter="10">
      <el-col :span="24">
        <el-form-item :label="$t('type')" prop="type">
          <el-input
            v-model="form.type"
            :placeholder="$t('kylinLang.common.pleaseInput')">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row> -->
    <el-form-item :label="$t('authentication')">
      <el-row :gutter="10" class="authentication-content">
        <!-- <el-col class="form-item-title" :span="24">{{$t('Authentication')}}</el-col> -->
        <el-col :span="24">
          <el-form-item :label="$t('awsAccessKey')"  prop="accessKey">
            <el-input
              v-model="form.accessKey"
              :placeholder="$t('kylinLang.common.pleaseInput')">
            </el-input>
          </el-form-item>
        </el-col>
        <el-col :span="24">
          <el-form-item :label="$t('awsSecretKey')" prop="secretKey">
            <el-input
              v-model="form.secretKey"
              :placeholder="$t('kylinLang.common.pleaseInput')">
            </el-input>
          </el-form-item>
        </el-col>
        <el-col :span="24" class="ksd-mt-10">
          <el-form-item>
              <el-button :disabled="!form.accessKey || !form.secretKey" :loading="testConnLoading" size="medium" @click="testConn">{{$t('testConnect')}}</el-button>
              <span class="ky-success ksd-ml-10" v-if="testConnResult===true"><i class="el-icon-success ksd-mr-4"></i>{{$t('connectSuccess')}}</span>
              <div class="ksd-lineheight-2 ksd-mt-4" v-if="testConnResult===false">
                <el-alert
                  :title="$t('connectFail') + ' ' + testConnMsg"
                  type="error"
                  :closable="false"
                  show-icon>
                </el-alert>
              </div>
          </el-form-item>
        </el-col>
      </el-row>
    </el-form-item>
    <el-row :gutter="10">
      <el-col :span="24">
        <el-form-item :label="$t('createTableKind')">
          <el-radio-group v-model="form.addTableType">
            <el-radio :label="0">{{$t('guideMode')}}</el-radio>
            <el-radio :label="1">{{$t('expertMode')}}</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-col>
    </el-row>
  </el-form>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
import { handleSuccess, handleError, collectObject } from '../../../../../util'
// import { validate, fieldVisiableMaps } from './handler'

@Component({
  methods: {
    ...mapActions({
      verifyConnection: 'VERIFY_CSV_CONN'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
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
  testConnLoading = false
  testConnResult = null
  testConnMsg = ''
  hashType = {
    's3a': 'AWS_S3_KEY'
  }
  autoFulfillType () {
    this.form.type = this.hashType[this.form.url && this.form.url.split('://')[0] || ''] || 'AWS_S3_KEY'
  }
  get rules () {
    return {
      url: [{ message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur', required: true }],
      type: [{ message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur', required: true }],
      accessKey: [{ message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur', required: true }],
      secretKey: [{ message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur', required: true }]
    }
  }
  testConn () {
    this.testConnLoading = true
    this.testConnResult = null
    this.testConnMsg = ''
    let connOptions = {
      credential: collectObject(this.form, ['type', 'accessKey', 'secretKey'], true),
      ...collectObject(this.form, ['type', 'url'])
    }
    this.verifyConnection(connOptions).then((res) => {
      handleSuccess(res, (data, code, msg) => {
        this.testConnLoading = false
        this.testConnResult = data
        this.testConnMsg = msg || ''
      })
    }, (res) => {
      handleError(res)
      this.testConnLoading = false
    })
  }
  isFieldVisiable (fieldName) {
    // return fieldVisiableMaps[this.editType].includes(fieldName)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.source-csv {
  padding: 20px 20px 0;
  .authentication-content {
    background: @table-stripe-color;
    padding:10px;
  }
}
</style>
