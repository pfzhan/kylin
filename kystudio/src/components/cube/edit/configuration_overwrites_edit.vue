<template>
<div class="overwrites-edit ksd-mrl-20">
  <div>
    <p class="overview-title ksd-fs-16">Cubing Engine
      <common-tip :content="$t('engineTip')" >
        <i class="el-icon-question ksd-fs-10"></i>
      </common-tip>
    </p>
    <p class="ksd-mt-20 ksd-ml-40 ksd-pl-40">Engine Type
      <el-select class="ksd-ml-10" size="medium" v-model="cubeDesc.engine_type">
        <el-option  v-for="item in engineType"
        :key="item.value"
        :label="item.name"
        :value="item.value"></el-option>
      </el-select>
    </p>
  </div>
  <div class="ky-line ksd-mt-20 ksd-mb-20"></div>
  <el-row>
    <el-col :span="24">
      <div>
        <p class="overview-title ksd-fs-16 ksd-lineheight-32 ksd-mr-10 ksd-fleft">{{$t('cubeDefault')}}</p>
        <el-button size="medium" type="primary" plain  icon="el-icon-plus" @click.native="addNewProperty">Configuration
        </el-button>
      </div>
    </el-col>
  </el-row>

  <el-row>
    <el-col :span="24" class="ksd-mt-10 ksd-mb-16">
      <p><span>{{$t('tip')}}:</span>
      {{$t('propertyTip')}}</p>
    </el-col>
  </el-row>

  <el-row v-for="(property, index) in convertedProperties" :key="index" :gutter="10" class="ksd-mtb-10 properties-block">
    <el-col :span="1" style="width:25px;">
      <el-checkbox class="ksd-lineheight-32" v-show="property.isDefault" v-model="property.checked" @change="changeProperty()"></el-checkbox>
    </el-col>
    <el-col :span="10" :offset="property.isDefault?0:1">
      <el-input size="medium" v-model="property.key" :disabled="property.isDefault" @change="changeProperty()"></el-input>
    </el-col>
    <el-col :span="10">
      <el-input size="medium" v-model="property.value" :disabled="!property.checked" @change="changeProperty()"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button icon="el-icon-delete" size="medium" v-if="!property.isDefault" @click.native="removeProperty(index)"></el-button>
    </el-col>
  </el-row>

  <el-row class="ksd-mt-40 ksd-mb-16">
    <el-col :span="24">
      <div class="cube-config">
        <p class="overview-title ksd-fs-16 ksd-lineheight-32 ksd-mr-10 ksd-fleft">{{$t('jobRelated')}}</p>
        <el-button size="medium" type="primary" plain icon="el-icon-plus" @click.native="addNewJobProperty">Configuration
        </el-button>
      </div>
    </el-col>
  </el-row>
  <el-row class="ksd-mt-10 ksd-mb-10 job-setting" v-for="(property, index) in jobConvertedProperties" :key="index" :gutter="10">
    <el-col :span="7">
      <el-input size="medium"  :disabled="true" placeholder="kylin.job.mr.config.override">
      </el-input>
    </el-col>
    <el-col :span="7">
      <el-input size="medium" v-model="property.key" :disabled="property.isDefault" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="7">
      <el-input size="medium" v-model="property.value" :disabled="!property.checked" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button icon="el-icon-delete" size="medium" v-if="!property.isDefault" @click.native="removeJobProperty(index)"></el-button>
    </el-col>
  </el-row>

  <el-row class="ksd-mt-40 ksd-mb-16">
    <el-col :span="24">
      <div class="cube-config">
        <p class="overview-title ksd-fs-16 ksd-lineheight-32 ksd-mr-10 ksd-fleft">{{$t('hiveRelated')}}</p>
        <el-button size="medium" type="primary" plain icon="el-icon-plus" @click.native="addNewHiveProperty">Configuration
        </el-button>
      </div>
    </el-col>
  </el-row>
  <el-row v-for="(property, index) in hiveConvertedProperties" :key="index" :gutter="10" class="ksd-mt-10 ksd-mb-10 hive-setting">
    <el-col :span="7">
      <el-input size="medium"  :disabled="true" placeholder="kylin.hive.config.override">
      </el-input>
    </el-col>
    <el-col :span="7">
      <el-input size="medium" v-model="property.key" :disabled="property.isDefault" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="7">
      <el-input size="medium" v-model="property.value" :disabled="!property.checked" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button icon="el-icon-delete" size="medium" v-if="!property.isDefault" @click.native="removeHiveProperty(index)"></el-button>
    </el-col>
  </el-row>
</div>
</template>

<script>
import { fromObjToArr } from '../../../util/index'
import { mapActions } from 'vuex'
import { engineTypeKylin, engineTypeKap } from '../../../config/index'
export default {
  name: 'configurationOverwrites',
  props: ['cubeDesc'],
  data () {
    return {
      convertedProperties: [],
      hiveConvertedProperties: [],
      jobConvertedProperties: [],
      properties: fromObjToArr(this.cubeDesc.override_kylin_properties)
    }
  },
  methods: {
    ...mapActions({
      loadConfig: 'LOAD_DEFAULT_CONFIG'
    }),
    addNewProperty: function () {
      this.convertedProperties.unshift({checked: true, key: '', value: '', isDefault: false})
      this.$set(this.cubeDesc.override_kylin_properties, '', '')
    },
    removeProperty: function (index) {
      this.$delete(this.cubeDesc.override_kylin_properties, this.convertedProperties[index].key)
      this.convertedProperties.splice(index, 1)
    },
    changeProperty: function () {
      let strategy = this.cubeDesc.override_kylin_properties['kap.smart.conf.aggGroup.strategy']
      this.cubeDesc.override_kylin_properties = {
        'kap.smart.conf.aggGroup.strategy': strategy
      }
      this.convertedProperties.forEach((item, index) => {
        if (item.checked) {
          this.$set(this.cubeDesc.override_kylin_properties, this.convertedProperties[index].key, this.convertedProperties[index].value)
        }
      })
      this.jobConvertedProperties.forEach((item, index) => {
        if (item.checked) {
          this.$set(this.cubeDesc.override_kylin_properties, 'kylin.job.mr.config.override.' + this.jobConvertedProperties[index].key, this.jobConvertedProperties[index].value)
        }
      })
      this.hiveConvertedProperties.forEach((item, index) => {
        if (item.checked) {
          this.$set(this.cubeDesc.override_kylin_properties, 'kylin.hive.config.override.' + this.hiveConvertedProperties[index].key, this.hiveConvertedProperties[index].value)
        }
      })
    },
    addNewJobProperty: function () {
      this.jobConvertedProperties.unshift({checked: true, key: '', value: '', isDefault: false})
      this.$set(this.cubeDesc.override_kylin_properties, '', '')
    },
    removeJobProperty: function (index) {
      this.$delete(this.cubeDesc.override_kylin_properties, this.jobConvertedProperties[index].key)
      this.jobConvertedProperties.splice(index, 1)
    },
    addNewHiveProperty: function () {
      this.hiveConvertedProperties.unshift({checked: true, key: '', value: '', isDefault: false})
      this.$set(this.cubeDesc.override_kylin_properties, '', '')
    },
    removeHiveProperty: function (index) {
      this.$delete(this.cubeDesc.override_kylin_properties, this.hiveConvertedProperties[index].key)
      this.hiveConvertedProperties.splice(index, 1)
    },
    initProperty: function () {
      let defaultConfigs = fromObjToArr(this.$store.state.config.defaultConfig.cube)
      defaultConfigs.forEach((config) => {
        this.convertedProperties.push({checked: false, key: config.key, value: config.value, isDefault: true})
      })
      for (let key of Object.keys(this.cubeDesc.override_kylin_properties)) {
        if (key.indexOf('kylin.job.mr.config.override.') === 0) {
          let keyString = key.slice(29)
          this.jobConvertedProperties.push({checked: true, key: keyString, value: this.cubeDesc.override_kylin_properties[key], isDefault: false})
        } else if (key.indexOf('kylin.hive.config.override.') === 0) {
          let keyString = key.slice(27)
          this.hiveConvertedProperties.push({checked: true, key: keyString, value: this.cubeDesc.override_kylin_properties[key], isDefault: false})
        } else {
          let isDefault = false
          this.convertedProperties.forEach((property, index) => {
            if (key === property.key) {
              this.$set(property, 'value', this.cubeDesc.override_kylin_properties[key])
              this.$set(property, 'checked', true)
              this.$set(property, 'isDefault', true)
              isDefault = true
            }
          })
          if (!isDefault) {
            if (key.indexOf('kap.smart.conf.aggGroup.strategy') !== 0) {
              this.convertedProperties.push({checked: true, key: key, value: this.cubeDesc.override_kylin_properties[key], isDefault: false})
            }
          }
        }
      }
    }
  },
  created: function () {
    this.loadConfig('cube').then(() => {
      this.initProperty()
    })
  },
  computed: {
    engineType () {
      if (+this.cubeDesc.storage_type === 2) {
        return engineTypeKylin
      } else if (+this.cubeDesc.storage_type === 99 || +this.cubeDesc.storage_type === 100) {
        return engineTypeKap
      }
    }
  },
  locales: {
    'en': {tip: 'Tips', propertyTip: 'Cube configuration will overwrite configuration in kylin.properties', addConfiguration: 'Add Configuration', engineTip: 'Select cube engine for building cube.', cubeDefault: 'Cube  Configuration', jobRelated: 'Job Related Configuration', hiveRelated: 'Hive Related Configuration'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube的属性值将会覆盖kylin.properties中的属性值', addConfiguration: '添加配置', engineTip: '选择一个Cube构建引擎。', cubeDefault: 'Cube配置', jobRelated: 'Job相关配置', hiveRelated: 'Hive相关配置'}
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .overwrites-edit{
    margin-bottom: 50px;
    .overview-title {
      color: @text-title-color;
      font-weight: bold;
    }
    .properties-block {
      .el-col-1 {
        text-align: center;
        &:first-child {
          width: 25px;
        }
        &:last-child {
          width: 56px;
        }
      }
      .el-col-offset-1 {
        margin-left: 25px;
      }
    }
    .job-setting,
    .hive-setting {
      .el-col-1 {
        text-align: center;
        width: 56px;
      }
    }
  }
</style>
