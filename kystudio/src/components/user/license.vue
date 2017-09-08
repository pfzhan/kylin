<template>
<div>
  <el-form label-position="top" ref="licenseForm" class="licenseBox">
    <el-input v-show="showTextArea" class="textArea" wrap="off"  ref="enterLicense" @blur="blurInput" :placeholder="$t('enterLicense')" v-model="licenseNumber" type="textarea" :row="5">
    </el-input>
    <el-input v-show="showInput" @focus="inputLicense"  :disabled="useFile" :placeholder="$t('enterLicense')" v-model="licenseNumber" multiple="multiple">
      <template slot="append">
        <el-upload action="https://jsonplaceholder.typicode.com/posts/" :file-list="fileList" :on-success="selectFile" :on-remove="removeFile"  :on-progress="changeFile" :multiple="false" >
          <el-button>Browse</el-button>
        </el-upload>
      </template>
    </el-input>  
  </el-form> 
</div>
</template>
<script>
import changeLang from '../common/change_lang'
import help from '../common/help'
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
    selectFile (response, file, fileList) {
      this.fileList = fileList
      this.licenseNumber = 'license'
      this.useFile = true
    },
    removeFile (file, fileList) {
      this.fileList = fileList
      this.useFile = false
      this.licenseNumber = ''
    },
    changeFile (event, file, fileList) {
      this.fileList = fileList
      if (fileList.length > 1) {
        fileList.splice(0, 1)
      }
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
      this.$emit('validSuccess', {content: this.licenseNumber, file: this.fileList, useFile: this.useFile})
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
      background-color:#a2a2a2;
      border-style:none;
      position: static;
      .el-button {
        margin: 0 0 0 0;
        padding: 0 0 0 0;
        border-style: none;
      }
    }
    .el-upload-list {
      position: absolute;
      margin-top: 5px;
      left: 0px;
      li {
        width: 100%
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
