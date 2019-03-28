<template>
  <el-form
    ref="form"
    class="source-csv"
    label-position="top"
    size="medium"
    :model="form"
    :rules="rules">
    <el-row :gutter="10">
      <el-col :span="12">
        <el-form-item :label="$t('database')" prop="tableData.database">
          <el-input
            style="width:100%;"
            v-model="form.tableData.database"
            :placeholder="$t('kylinLang.common.pleaseInput')">
          </el-input>
        </el-form-item>
      </el-col>
      <el-col :span="12">
        <el-form-item :label="$t('name')" prop="tableData.name">
          <el-input
            style="width:100%"
            v-model="form.tableData.name"
            :placeholder="$t('kylinLang.common.pleaseInput')">
          </el-input>
        </el-form-item>
      </el-col>
    </el-row>
    <el-row :gutter="10" >
      <el-col :span="24">
        <el-form-item :label="$t('tableConstructure')">
          <el-table :data="form.tableData.columns" border>
            <el-table-column
              prop="id"
              label="ID"
              width="40">
            </el-table-column>
            <el-table-column
              prop="name"
              :label="$t('columnName')"
              width="180">
              <template slot-scope="scope">
                <el-input v-model="scope.row.name" size="medium"> </el-input>
              </template>
            </el-table-column>
            <el-table-column
              prop="datatype"
              :label="$t('columnType')"
              width="120">
              <template slot-scope="scope">
                <el-select v-model="scope.row.datatype" size="medium">
                  <el-option
                    v-for="item in columnTypes"
                    :key="item"
                    :label="item"
                    :value="item">
                  </el-option>
                </el-select>
              </template>
            </el-table-column>
            <el-table-column
              prop="sample"
              :label="$t('sampleData')">
              <template slot-scope="scope">
                {{scope.row.sample && scope.row.sample.join(',') || ''}}
              </template>
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
      loadCSVScheme: 'LOAD_CSV_SCHEME'
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
  columnTypes = ['byte', 'short', 'integer', 'long', 'float', 'double', 'decimal', 'string', 'binary', 'boolean', 'timestamp', 'date']
  get rules () {
    return {
      'tableData.database': [
        { validator: this.validateName, trigger: 'blur' }
      ],
      'tableData.name': [
        { validator: this.validateName, trigger: 'blur' }
      ]
    }
  }
  validateName (rule, value, callback) {
    if (value === '') {
      callback(new Error('请输入'))
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
      callback(new Error('格式为数字字母下划线的任意组合，不能以数字开头!'))
    } else {
      callback()
    }
  }
  isFieldVisiable (fieldName) {
    // return fieldVisiableMaps[this.editType].includes(fieldName)
  }
  loadingCover () {
    return this.$loading({
      target: document.querySelector('.source-csv'),
      text: this.$t('loadText')
    })
  }
  mounted () {
    let sampleOptions = {
      credential: collectObject(this.form, ['type', 'accessKey', 'secretKey'], true),
      ...collectObject(this.form, ['type', 'url', 'separatorChar', 'quoteChar'])
    }
    let loadingObj = this.loadingCover()
    this.$emit('lockStep', true)
    this.loadCSVScheme(sampleOptions).then((res) => {
      loadingObj.close()
      this.$emit('lockStep', false)
      handleSuccess(res, (data) => {
        let sampleData = this.form.sampleData.body && this.form.sampleData.body.slice(0, 3) || []
        this.form.tableData.columns = this.form.sampleData.header.map((c, i) => {
          return {
            id: i,
            name: c,
            datatype: data[i],
            sample: sampleData.map((s) => {
              return s[i]
            })
          }
        })
      })
    }, (res) => {
      loadingObj.close()
      this.$emit('lockStep', false)
      handleError(res)
    })
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
</style>
