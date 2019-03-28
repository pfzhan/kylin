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
        <el-form-item :label="$t('separatorChar')" prop="separatorChar">
          <el-radio-group v-model="form.separatorChar" @change="getSampleData">
            <el-radio label=",">{{$t('comma')}}</el-radio>
            <el-radio label="\t">{{$t('tab')}}</el-radio>
            <el-radio label=";">{{$t('semicolon')}}</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10">
      <el-col :span="24">
        <el-form-item :label="$t('quoteChar')" prop="quoteChar">
          <el-input
            :maxlength="1"
            v-model="form.quoteChar"
            @input="getSampleDataAsync"
            style="width:398px"
            :placeholder="$t('kylinLang.common.pleaseInput')">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10" >
      <el-col :span="24">
        <el-form-item :label="$t('sampleData')">
          <el-checkbox v-model="form.useFirstLine">{{$t('useFirstLine')}}</el-checkbox>
          <el-table :data="renderSampleData.body" border height="280px">
            <el-table-column
              v-for="(h, i) in renderSampleData.header"
              :prop="''+i"
              :key="h"
              :label="h"
              :min-width="15*(h.length || 0)"
              >
            </el-table-column>
          </el-table>
        </el-form-item>
      </el-col>
    </el-row>
  </el-form>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import locales from './locales'
import { handleSuccess, collectObject, handleError } from '../../../../../util'
// import { validate, fieldVisiableMaps } from './handler'

@Component({
  methods: {
    ...mapActions({
      getCSVSampleData: 'GET_CSV_SAMPLE'
    })
  },
  props: {
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
      quoteChar: [{
        validator: this.validateQuote, trigger: 'change'
      }]
    }
  }
  ST = null
  validateQuote (rule, value, callback) {
    if (value.length > 1) {
      callback(new Error(this.$t('quoteCharValidErrorTip')))
    } else {
      callback()
    }
  }
  getSampleDataAsync () {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.getSampleData()
    }, 500)
  }
  get renderSampleData () {
    if (!(this.sampleData && this.sampleData.length)) {
      return []
    }
    let result = []
    if (this.form.useFirstLine) {
      result = {
        header: this.sampleData[0],
        body: this.sampleData.slice(1)
      }
    } else {
      let headerColumnName = this.sampleData[0].map((c, i) => {
        return 'column' + (i + 1)
      })
      result = {
        header: headerColumnName,
        body: this.sampleData
      }
    }
    this.form.sampleData = result
    return result
  }
  sampleData = []
  isLoadingSample = false
  loadingCover () {
    return this.$loading({
      target: document.querySelector('.source-csv'),
      text: this.$t('loadText')
    })
  }
  getSampleData () {
    let sampleOptions = {
      credential: collectObject(this.form, ['type', 'accessKey', 'secretKey'], true),
      ...collectObject(this.form, ['type', 'url', 'separatorChar', 'quoteChar'])
    }
    let loadingObj = this.loadingCover()
    this.$emit('lockStep', true)
    this.getCSVSampleData(sampleOptions).then((res) => {
      loadingObj.close()
      this.$emit('lockStep', false)
      handleSuccess(res, (data) => {
        this.sampleData = data
      })
    }, (res) => {
      this.$emit('lockStep', false)
      loadingObj.close()
      handleError(res)
    })
  }
  mounted () {
    this.getSampleData()
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
</style>
