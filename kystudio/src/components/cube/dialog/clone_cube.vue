<template>
<div class="clone-cube">
  <el-form label-position="left" label-width="130px" :model="newCube" :rules="rules" ref="cloneCubeForm">
    <el-form-item :label="$t('newCubeName')" prop="cubeName">
      <el-input size="medium" v-model="newCube.cubeName"></el-input>
    </el-form-item> 
 </el-form> 
</div>     
</template>
<script>
export default {
  name: 'clone_cube',
  props: ['cubeDesc'],
  data () {
    return {
      newCube: {
        originalName: this.cubeDesc.name,
        cubeName: this.cubeDesc.name + '_clone',
        project: this.cubeDesc.project
      },
      rules: {
        cubeName: [
            { required: true, message: '', trigger: 'blur' }
        ]
      }
    }
  },
  watch: {
    cubeDesc (cubeDesc) {
      this.newCube.originalName = this.cubeDesc.name
      this.newCube.cubeName = this.cubeDesc.name + '_clone'
      this.newCube.project = this.cubeDesc.project
    }
  },
  created () {
    this.$on('cloneCubeFormValid', (t) => {
      this.$refs['cloneCubeForm'].validate((valid) => {
        if (valid) {
          this.$emit('validSuccess', this.newCube)
        }
      })
    })
  },
  locales: {
    'en': {newCubeName: 'New Cube Name', tip: 'The clone target should be in current project.', requiredName: 'The cube name is required'},
    'zh-cn': {newCubeName: '新的Cube名称', tip: '克隆目标只能在当前项目下。', requiredName: '请输入Cube名称'}
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .clone-cube {
    .el-input {
      width: 220px;
    }
  }
</style>
