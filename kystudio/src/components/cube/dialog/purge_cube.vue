<template>
  <div id="purge-cube">
    <el-form  :model="mutilLevel" :rules="rules" label-position="top" ref="purgeCubeForm">
      <div v-if="cubeDesc.multilevel_partition_cols.length > 0">
        <el-form-item :label="$t('kylinLang.model.primaryPartitionColumn')" >
          <el-tag>{{cubeDesc.multilevel_partition_cols[0]}}</el-tag>
        </el-form-item>
        <el-form-item :label="$t('partitionValues')" prop="mpValues">
          <el-select v-model="mutilLevel.mpValues" filterable :placeholder="$t('kylinLang.common.pleaseFilter')">
            <el-option
              v-for="item in mpValuesList"
              :key="item.name"
              :label="item.name"
              :value="item.name">
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
import { objectArraySort } from '../../../util/index'
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
            this.mpValuesList = objectArraySort(data, true, 'name')
            if (this.mpValuesList.length > 0) {
              this.mutilLevel.mpValues = this.mpValuesList[0].name
            } else {
              this.mutilLevel.mpValues = ''
            }
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
          this.mpValuesList = objectArraySort(data, true, 'name')
          if (this.mpValuesList.length > 0) {
            this.mutilLevel.mpValues = this.mpValuesList[0].name
          } else {
            this.mutilLevel.mpValues = ''
          }
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
    .el-form {
      .el-form-item__label{
        float: left!important;
      }
      .el-form-item{
        display: grid;
        height: 50px;
        margin: 15px 0px 0px 0px;
        .el-form-item__label {
          padding: 0 0 5px;
        }
        span {
          width: 100%;
          height: 36px;
          line-height: 36px;
          padding: 0px 10px 0px 10px;
        }
      }
      .el-select {
        margin-left: 0px;
        width: 100%;
      }
      p {
        margin-top: 15px;
      }
    }
  }
</style>
