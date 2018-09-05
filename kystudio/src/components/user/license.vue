<template>
<div>
  <el-form label-position="top" ref="licenseForm" class="licenseBox">
    <el-row>
      <el-col :span="6">
        <el-upload :headers="uploadHeader" :action="actionUrl" :file-list="fileList" ref="upload" :on-remove="removeFile"  :on-change="changeFile" :multiple="false" :auto-upload="false" :on-success="successFile" :on-error="errorFile" :show-file-list="false">
          <el-button slot="trigger" type="primary" ref="browse">Browse</el-button>
        </el-upload>
      </el-col>
      <el-col :span="18">
        <div @click="inputLicense()" class="licen_name" :class="{ like_placeholder: isPlaceholder }">{{fileName}}</div>
      </el-col>
    </el-row>

    <p class="ksd-fs-12 ">or</p>
    <el-input class="textArea" wrap="off" :placeholder="$t('enterLicense')"  ref="enterLicense" v-model="licenseNumber" type="textarea" :row="5">
    </el-input>

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
      licenseNumber: '',
      fileList: [],
      fileName: this.$t('uploadLicense'),
      useFile: false,
      lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'en'
    }
  },
  methods: {
    ...mapActions({
      saveLicenseContent: 'SAVE_LICENSE_CONTENT'
    }),
    successFile (response, file, fileList) {
      this.$store.state.system.serverAboutKap['code'] = response.code
      this.$store.state.system.serverAboutKap['msg'] = response.msg
      this.$store.state.system.serverAboutKap['kap.dates'] = response.data['kap.dates']
      this.emitResult(true)
    },
    errorFile (err, file, fileList) {
      handleError({data: err})
      this.emitResult(false)
    },
    removeFile (file, fileList) {
      this.useFile = false
      this.fileName = ''
    },
    changeFile (file, fileList) {
      if (fileList.length > 1) {
        fileList.splice(0, 1)
      }
      this.fileName = fileList[0].name
      this.useFile = true
    },
    inputLicense (event) {
      this.$refs.upload.$el.querySelectorAll('.el-upload')[0].click()
    },
    emitResult (result) {
      this.$emit('validSuccess', result)
    }
  },
  computed: {
    actionUrl () {
      return apiUrl + 'kap/system/license/file'
    },
    uploadHeader () {
      if (this.lang === 'en') {
        return {'Accept-Language': 'en'}
      } else {
        return {'Accept-Language': 'cn'}
      }
    },
    isPlaceholder () {
      if (this.fileName === 'Please upload the license.' || this.fileName === '请上传输入许可证') {
        return true
      }
      return false
    }
  },
  watch: {
    updateLicenseVisible (updateLicenseVisible) {
      this.fileList.splice(0, 1)
      this.licenseNumber = ''
      this.useFile = false
      this.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'en'
    }
  },
  created () {
    this.$on('licenseFormValid', (t) => {
      if (this.useFile) {
        this.$refs.upload.submit()
      } else {
        this.saveLicenseContent(this.licenseNumber).then((res) => {
          handleSuccess(res, (data, code, msg) => {
            this.$store.state.system.serverAboutKap['code'] = code
            this.$store.state.system.serverAboutKap['msg'] = msg
            this.$store.state.system.serverAboutKap['kap.dates'] = data['kap.dates']
            this.emitResult(true)
          })
        }, (res) => {
          handleError(res, () => {
            this.emitResult(false)
          })
        })
      }
    })
  },
  components: {
    'kap-change-lang': changeLang,
    'kap-help': help
  },
  locales: {
    'en': {uploadLicense: 'Please upload the license.', enterLicense: 'Please enter the license.', upload: 'Upload'},
    'zh-cn': {uploadLicense: '请上传输入许可证', enterLicense: '请手动输入许可证', upload: '上传'}
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .licenseBox {
    .el-button {
      background-color: @base-color;
      color: @fff;
      font-size: 15px;
      width: 80px;
      border: none;
      border-top-right-radius: 0;
      border-bottom-right-radius: 0;
      border-bottom: 1px solid @base-color;
    }
    .like_placeholder {
      color: @text-secondary-color;
    }
    .licen_name {
      width: 205px;
      height: 34px;
      border-radius: 2px;
      border-top-left-radius: 0;
      border-bottom-left-radius: 0;
      border: 1px solid @line-border-color;
      line-height: 34px;
      padding: 0 15px;
      cursor: pointer;
    }
    p {
      text-align: center;
    }
    .textArea {
      .el-textarea__inner {
        overflow-x: scroll;
        overflow-y: scroll;
      }
    }
  }
</style>
