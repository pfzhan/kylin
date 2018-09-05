<template>
  <div id="purge-cube">
    <el-form  :model="mutilLevel" :rules="rules" label-position="top" ref="purgeCubeForm">
      <div v-if="cubeDesc.multilevel_partition_cols.length > 0">
        <el-form-item :label="$t('kylinLang.model.primaryPartitionColumn')" >
          <el-input size="medium" v-model="cubeDesc.multilevel_partition_cols[0]" :disabled="true"> </el-input>
        </el-form-item>
        <el-form-item :label="$t('partitionValues')" prop="mpValues">
          <el-select size="medium" v-model="mutilLevel.mpValues" filterable :placeholder="$t('kylinLang.common.pleaseFilter')">
            <el-option
              v-for="item in mpValuesList"
              :key="item.name"
              :label="item.name"
              :value="item.name">
            </el-option>
          </el-select>
        </el-form-item>
      </div>
      <p class="ksd-mb-20">{{$t('purgeCube')}}</p>
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
  @import '../../../assets/styles/variables.less';
  #purge-cube{
    .el-form {
      .el-input__inner {
        width: 300px;
      }
    }
  }
</style>
