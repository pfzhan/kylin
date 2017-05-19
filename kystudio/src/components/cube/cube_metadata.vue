<template>
    <div class="table_margin">   
      <editor v-model="json"  theme="chrome" class="ksd-mt-20" width="100%" height="400" ></editor>
      <el-button class="button_right" v-if="extraoption.type==='edit'" @click="update">{{$t('save')}}</el-button>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
import editor from 'vue2-ace-editor'
export default {
  name: 'cubeMetadata',
  props: ['extraoption'],
  data () {
    return {
      json: ''
    }
  },
  components: {
    editor
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      updateCube: 'UPDATE_CUBE'
    }),
    update: function () {
      let _this = this
      _this.$confirm('确认保存Cube？', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        _this.updateCube({project: _this.extraoption.project, cubeName: _this.extraoption.cubeName, cubeDescData: _this.json}).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            _this.$message({
              type: 'success',
              message: '保存成功!'
            })
          })
          _this.$emit('removetabs', 'edit' + _this.extraoption.cubeName)
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            console.log(status, 30000)
            // if (status === 404) {
            //   _this.$router.replace('access/login')
            // }
          })
        })
      }).catch((e) => {
        console.log(e)
        _this.$message({
          type: 'info',
          message: '已取消保存'
        })
      })
    }
  },
  created () {
    let _this = this
    if (_this.extraoption.type === 'view') {
      _this.json = JSON.stringify(_this.extraoption.cubeDesc)
    } else {
      _this.loadCubeDesc(_this.extraoption.cubeName).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.json = JSON.stringify(data[0])
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          console.log(status, 30000)
          // if (status === 404) {
          //   _this.$router.replace('access/login')
          // }
        })
      })
    }
  },
  locales: {
    'en': {save: 'Save'},
    'zh-cn': {save: '保存'}
  }
}
</script>
<style scoped>
 .button_right {
  float: right;
  margin:20px 20px 20px 20px;

 }
</style>
