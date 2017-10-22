<template>
  <div id="purge-cube">
    <el-form  :model="mutilLevel" :rules="rules" label-position="right" label-width="200px" ref="purgeCubeForm">
      <div v-if="cubeDesc.multilevel_partition_cols.length > 0">
        <el-form-item :label="$t('kylinLang.model.primaryPartitionColumn')" >
          <el-tag>{{cubeDesc.multilevel_partition_cols[0]}}</el-tag>
        </el-form-item>
        <el-form-item :label="$t('partitionValues')" prop="mpValues">
          <el-select v-model="mutilLevel.mpValues" filterable :placeholder="$t('kylinLang.common.pleaseFilter')">
            <el-option
              v-for="item in mpValuesList"
              :key="item"
              :label="item"
              :value="item">
            </el-option>
          </el-select>
        </el-form-item>
      </div>
      <p>{{$t('purgeCube')}}</p>
   </el-form> 
  </div>     
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
export default {
  name: 'purge_cube',
  props: ['cubeDesc'],
  data () {
    return {
      mutilLevel: {
        mpValues: ''
      },
      mpValuesList: [],
      rules: {
        mpValues: [
          { required: true, message: this.$t('secondaryPartitionNull'), trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    ...mapActions({
      getMPValues: 'GET_MP_VALUES'
    })
  },
  watch: {
    cubeDesc (cubeDesc) {
      if (this.cubeDesc.multilevel_partition_cols.length > 0) {
        this.getMPValues(this.cubeDesc.name).then((res) => {
          handleSuccess(res, (data) => {
            this.mpValuesList = data.sort()
            this.mutilLevel.mpValues = this.mpValuesList[0]
          })
        }, (res) => {
          handleError(res)
        })
      }
    }
  },
  created () {
    if (this.cubeDesc.multilevel_partition_cols.length > 0) {
      this.getMPValues(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data) => {
          this.mpValuesList = data.sort()
          this.mutilLevel.mpValues = this.mpValuesList[0]
        })
      }, (res) => {
        handleError(res)
      })
    }
    this.$on('purgeCubeFormValid', (t) => {
      this.$refs['purgeCubeForm'].validate((valid) => {
        if (valid) {
          this.$emit('validSuccess', this.mutilLevel)
        }
      })
    })
    this.$on('resetPurgeCubeForm', (t) => {
      this.$refs['purgeCubeForm'].resetFields()
    })
  },
  locales: {
    'en': {purgeCube: 'Are you sure to purge this cube? ', secondaryPartitionNull: 'Please select partition value.', partitionValues: 'Partition Value'},
    'zh-cn': {purgeCube: '确定要清理此Cube?', secondaryPartitionNull: '请选择分区值。', partitionValues: '分区值'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  #purge-cube{
    .el-form-item__label{
      float: left!important;
    }
    .el-form-item{
      height: 50px;
      margin-top: 10px;
      margin-bottom: 10px;
    }
    p {
      margin-left: 20px;
    }
    .el-date-editor{
      height: 36px;
      padding: 0;
    }
    .el-input {
      margin-left: 0px;
    }
  }
</style>
