<template>
<div>
  <el-form label-position="top" ref="licenseForm" class="licenseBox">
    <el-input v-show="showTextArea" class="textArea" wrap="off"  ref="enterLicense" @blur="blurInput" :placeholder="$t('enterLicense')" v-model="licenseNumber" type="textarea" :row="5">
    </el-input>
    <el-input v-show="showInput" @focus="inputLicense"  :disabled="useFile" :placeholder="$t('enterLicense')" v-model="licenseNumber">
      <template slot="append">
        <el-upload :action="actionUrl" :file-list="fileList" ref="upload" :on-remove="removeFile"  :on-change="changeFile" :multiple="false" :auto-upload="false" :on-success="successFile" :on-error="errorFile">
          <el-button slot="trigger">Browse</el-button>
        </el-upload>
      </template>
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
      useFile: false,
      showInput: true,
      showTextArea: false
    }
  },
  methods: {
    ...mapActions({
      saveLicenseContent: 'SAVE_LICENSE_CONTENT'
    }),
    successFile (response, file, fileList) {
      this.$store.state.system.serverAboutKap['kap.dates'] = response.data['kap.dates']
      this.emitResult(true)
    },
    errorFile (err, file, fileList) {
      handleError({data: err})
      this.emitResult(false)
    },
    removeFile (file, fileList) {
      this.useFile = false
      this.licenseNumber = ''
    },
    changeFile (file, fileList) {
      if (fileList.length > 1) {
        fileList.splice(0, 1)
      }
      this.licenseNumber = 'LICENSE'
      this.useFile = true
    },
    inputLicense (event) {
      this.showInput = false
      this.showTextArea = true
      this.$nextTick(() => {
        this.$refs['enterLicense'].$el.children[0].focus()
        this.$set(this.$refs['enterLicense'].$el.children[0], 'wrap', 'off')
      })
    },
    blurInput (event) {
      this.showInput = true
      this.showTextArea = false
    },
    emitResult (result) {
      this.$emit('validSuccess', result)
    }
  },
  computed: {
    actionUrl () {
      return apiUrl + 'kap/system/license/file'
    }
  },
  watch: {
    updateLicenseVisible (updateLicenseVisible) {
      this.fileList.splice(0, 1)
      this.licenseNumber = ''
      this.useFile = false
    }
  },
  created () {
    this.$on('licenseFormValid', (t) => {
      if (this.useFile) {
        this.$refs.upload.submit()
      } else {
        this.saveLicenseContent(this.licenseNumber).then((res) => {
          handleSuccess(res, (data) => {
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
    'en': {enterLicense: 'Please upload or enter the license.', upload: 'Upload'},
    'zh-cn': {enterLicense: '请上传或手动输入许可证', upload: '上传'}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  .licenseBox {
    background: @grey-color;
    .el-input{
      width: 100%;
      display: inline-table;
      margin-top: 10px;
      position: relative;
    }
    .el-input-group__append {
      width: 8%;
      background-color:#767C9D;
      border-style:none;
      position: static;
      .el-button {
        margin: 0 0 0 0;
        padding: 0 0 0 0;
        border-style: none;
        font-size: 12px;
      }
    }
    .el-input-group__append:hover{
      ackground-color:#8C95BC;
    }
    .el-upload-list {
      display: inline-flex;
      position: absolute;
      margin-top: 5px;
      left: 0px;
      li {
        height: 25px;
        width: 100%;
        display: inline-block;
        margin-right: 40px;
      }
      .el-upload-list__item {
        transition: all .5s cubic-bezier(.55,0,.1,1);
        font-size: 12px;
        color: #48576a;
        line-height: 1.8;
        margin-top: 10px;
        box-sizing: border-box;
        border-radius: 4px;
        width: 100%;
        position: relative;
        a {
          text-decoration-line: none;
        }
      }
    }
    .textArea {
      .el-textarea__inner {
        overflow-x: scroll;
        overflow-y: scroll;
      }
    }
  }
</style>
