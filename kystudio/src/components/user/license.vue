<template>
<div class="license-box">
  <div class="ksd-mb-20">
    <p>
      <span>
        <span class="ky-fw-m">{{$t('validPeriod')}}</span>
         <span v-if="hasLicense" >{{licenseRange}}</span>
         <span v-else>{{$t('noLicense')}}</span>
      </span>
      <span class="ky-a-like" v-if="isRunInCloud" @click="applyLicense">{{$t('applyLicense')}}</span>
    </p>
  </div>
  <div class="license-type-switch">
    <el-radio-group v-model="addLicenseType" @change="switchLicenseType">
      <el-radio :label="true">{{$t('uploadLicense')}}</el-radio>
      <el-radio :label="false">{{$t('pastLicense')}}</el-radio>
    </el-radio-group>
  </div>
  <el-form :model="licenseForm" label-position="top" ref="licenseForm" class="ksd-mt-15">
    <el-form-item v-if="addLicenseType" prop="fileList" :rules="[{required: true, message: $t('pleaseSelectLicense'), trigger: 'change'}]">
      <input type="hidden" v-model="licenseForm.fileList" />
      <el-upload
        class="upload-area"
        drag
        :on-remove="handleRemove"
        :headers="uploadHeader"
        :action="actionUrl"
        :file-list="licenseForm.fileList"
        ref="upload"
        :on-change="changeFile"
        :multiple="false"
        :auto-upload="false"
        :on-success="successFile"
        :on-error="errorFile"
        :show-file-list="true">
        <i class="el-icon-ksd-add"></i>
        <div class="el-upload__text">{{$t('dropOrClickUpload')}}</div>
      </el-upload>
      <div class="file-error" v-if="fileSizeError">{{$t('filesSizeError')}}</div>
    </el-form-item>
    <el-form-item v-else prop="licenseContent" :rules="[{required: true, message: $t('pleaseEnterLicense'), trigger: 'blur'}]">
      <el-input class="textarea-input" v-focus="!addLicenseType" :placeholder="$t('pleaseEnterLicense')"  ref="enterLicense" v-model="licenseForm.licenseContent" type="textarea" :rows="6"></el-input>
    </el-form-item>
  </el-form>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import changeLang from '../common/change_lang'
import help from '../common/help'
import { apiUrl } from '../../config'
export default {
  name: 'license',
  props: ['updateLicenseVisible'],
  data () {
    return {
      addLicenseType: true,
      isRunInCloud: false,
      licenseForm: {
        fileList: [],
        licenseContent: ''
      },
      lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'en',
      fileSizeError: false
    }
  },
  methods: {
    ...mapActions({
      saveLicenseContent: 'SAVE_LICENSE_CONTENT',
      getHasRunInCloud: 'IS_CLOUD'
    }),
    applyLicense () {
      this.$emit('requestLicense')
    },
    switchLicenseType () {
      this.licenseForm.fileList = []
      this.licenseForm.licenseContent = ''
      this.fileSizeError = false
    },
    successFile (response, file, fileList) {
      Object.assign(this.$store.state.system.serverAboutKap, response.data)
      this.$store.state.system.serverAboutKap['code'] = response.code
      this.$store.state.system.serverAboutKap['msg'] = response.msg
      setTimeout(() => {
        this.emitResult(true)
      }, 1000)
    },
    errorFile (err, file, fileList) {
      try {
        let data = JSON.parse(err.message)
        handleError({data: data, status: err.status})
      } catch (e) {
        handleError({data: {}, status: err.status})
      }
      this.emitResult(false)
    },
    handleRemove () {
      this.licenseForm.fileList = []
      this.fileSizeError = false
    },
    changeFile (file, fileList) {
      // 多次上传后清理之前上传的内容
      if (fileList.length > 1) {
        fileList.splice(0, 1)
      }
      this.licenseForm.fileList = fileList
      // 输入内容后重新触发校验
      if (fileList.length > 0) {
        this.$refs.licenseForm.validateField('fileList')
        if (fileList[0].size > 5 * 1024 * 1024) { // 后端限制不能大于5M
          this.fileSizeError = true
          this.emitResult(false)
        } else {
          this.fileSizeError = false
        }
      } else {
        this.fileSizeError = false
      }
    },
    emitResult (result) {
      this.$emit('validSuccess', result)
    },
    license (obj) {
      if (!obj) {
        return 'N/A'
      } else {
        return obj
      }
    }
  },
  computed: {
    actionUrl () {
      return apiUrl + 'system/license/file'
    },
    uploadHeader () {
      if (this.lang === 'en') {
        return {'Accept-Language': 'en'}
      } else {
        return {'Accept-Language': 'cn'}
      }
    },
    hasLicense () {
      return this.serverAboutKap && this.serverAboutKap['ke.dates']
    },
    serverAboutKap () {
      return this.$store.state.system.serverAboutKap || {}
    },
    licenseRange () {
      let range = ''
      if (this.license(this.serverAboutKap && this.serverAboutKap['ke.dates']) !== 'N/A') {
        const dates = this.serverAboutKap['ke.dates'].split(',')
        range = dates[0] + ' ' + this.$t('kylinLang.query.to') + ' ' + dates[1]
      }
      return range
    }
  },
  watch: {
    updateLicenseVisible (updateLicenseVisible) {
      this.licenseForm.fileList = []
      this.licenseForm.licenseContent = ''
      this.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'en'
    },
    addLicenseType () {
      this.$refs.licenseForm.clearValidate()
    }
  },
  created () {
    this.getHasRunInCloud().then((res) => {
      handleSuccess(res, (data) => {
        this.isRunInCloud = data
      })
    }, (res) => {
      handleError(res)
    })
    this.$on('licenseFormValid', (t) => {
      this.$refs.licenseForm.validate((valid) => {
        if (valid && !this.fileSizeError) {
          if (this.addLicenseType) {
            this.$refs.upload.submit()
          } else {
            this.saveLicenseContent(this.licenseForm.licenseContent).then((res) => {
              handleSuccess(res, (data, code, msg) => {
                this.$store.state.system.serverAboutKap['code'] = code
                this.$store.state.system.serverAboutKap['msg'] = msg
                this.$store.state.system.serverAboutKap['ke.dates'] = data['ke.dates']
                this.emitResult(true)
              })
            }, (res) => {
              handleError(res)
              this.emitResult(false)
            })
          }
        } else {
          this.emitResult(false)
        }
      })
    })
  },
  components: {
    'kap-change-lang': changeLang,
    'kap-help': help
  },
  locales: {
    'en': {
      youcan: 'You can ',
      or: 'or',
      noLicense: 'No license',
      applyLicense: 'Apply Evaluation License',
      validPeriod: 'Valid Period: ',
      pastLicense: 'paste license content',
      uploadLicense: 'upload license file',
      enterLicense: 'Enter the license',
      pleaseEnterLicense: 'Please paste license content',
      pleaseSelectLicense: 'Please select a license file',
      upload: 'Upload',
      dropOrClickUpload: 'Drop file here or click to upload',
      filesSizeError: 'Supported file size is up to 5 MB, please check and upload again.'
    },
    'zh-cn': {
      youcan: '您可以',
      or: '或',
      noLicense: '尚无许可证',
      applyLicense: '申请许可证',
      validPeriod: '使用期限: ',
      pastLicense: '输入许可证内容',
      uploadLicense: '上传许可证文件',
      enterLicense: '手动输入许可证',
      pleaseEnterLicense: '请输入许可证内容',
      pleaseSelectLicense: '请选择许可证文件',
      upload: '上传',
      dropOrClickUpload: '将文件拖到此处，或点击上传',
      filesSizeError: '当前文件最大支持 5 MB，请检查后重新上传。'
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  @enterLicenseContentWidth:439px;
  .license-box {
    margin-bottom:7px;
    .el-form-item  {
      height:140px;
      width:@enterLicenseContentWidth;
      margin: 0 auto;
    }
    .upload-area {
      .el-upload-dragger {
        width:@enterLicenseContentWidth;
        height:100px;
        border-radius: 2px;
        .el-icon-ksd-add {
          margin-top:15px;
          margin-bottom:0px;
          font-size:32px;
          color:@text-disabled-color;
        }
        .el-upload__text {
          font-size:12px;
          line-height: 12px;
          color:@text-disabled-color;
        }
        &:hover {
          .el-icon-ksd-add, .el-upload__text {
            color: @base-color;
          }
        }
      }
      .el-upload-list__item:first-child {
        margin-top: 0px;
      }
    }
    .file-error {
      color: @error-color-1;
      font-size: 12px;
      height: 12px;
      line-height: 12px;
      margin-top: 5px;
    }
    .license-type-switch {
      font-size:14px;
      >div {
        width:@enterLicenseContentWidth;
        margin: 0 auto;
        text-align: left;
      }
    }
    .textarea-input {
      width:@enterLicenseContentWidth;
    }
    .el-upload-list {
      .el-upload-list__item-name {
        &:hover{
          text-decoration:none;
        }
      }
      display: inline;
      min-width:100px;
      margin: 0 auto;
      a {
        &:active{
          color:@text-title-color;
        }
      }
    }
    p {
      font-size:14px;
    }
    .textArea {
      .el-textarea__inner {
        overflow-x: scroll;
        overflow-y: scroll;
      }
    }
  }
</style>
