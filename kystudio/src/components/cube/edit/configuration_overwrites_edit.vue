<template>
<div id="overwrites">
  <div>
    <h2>Cubing Engine <common-tip :content="$t('engineTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip></h2>
    <p class="ksd-mt-14">Engine Type:
      <el-select v-model="cubeDesc.engine_type">
        <el-option  v-for="item in engineType"
        :key="item.value"
        :label="item.name"
        :value="item.value"></el-option>
      </el-select>
    </p>
  </div>
  <div class="line"></div>
  <el-row>
    <el-col :span="5">
      <div class="cube-config">
        <h2>{{$t('cubeDefault')}}</h2>
        <el-button type="default" icon="plus" @click.native="addNewProperty">
        </el-button>
      </div>
    </el-col>
  </el-row>

  <el-row>
    <el-col :span="24" style="font-size: 14px;margin-bottom: 10px;">
      <span style="color:#20a0ff;font-size: 14px;">{{$t('tip')}}:</span>
      {{$t('propertyTip')}}
    </el-col>
  </el-row>

  <el-row v-for="(property, index) in convertedProperties" :key="index" :gutter="20" class="row_padding">
    <el-col :span="1">
      <el-checkbox v-show="property.isDefault" v-model="property.checked" @change="changeProperty()"></el-checkbox>
    </el-col>
    <el-col :span="10" :offset="property.isDefault?0:1">
      <el-input  v-model="property.key" :disabled="property.isDefault" @change="changeProperty()"></el-input>
    </el-col>
    <el-col :span="10">
      <el-input  v-model="property.value" :disabled="!property.checked" @change="changeProperty()"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button type="delete" icon="minus" size="mini" v-if="!property.isDefault" @click.native="removeProperty(index)"></el-button>
    </el-col>
  </el-row>
  <div class="line"></div>
    <el-row>
    <el-col :span="12">
      <div class="cube-config">
        <h2>{{$t('jobRelated')}}</h2>
        <el-button type="default" icon="plus" @click.native="addNewJobProperty">
        </el-button>
      </div>
    </el-col>
  </el-row>
  <el-row v-for="(property, index) in jobConvertedProperties" :key="index" :gutter="20" class="row_padding">
    <el-col :span="7">
      <el-input   :disabled="true" placeholder="kylin.job.mr.config.override">
      </el-input>
    </el-col>
    <el-col :span="7">
      <el-input  v-model="property.key" :disabled="property.isDefault" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="7">
      <el-input  v-model="property.value" :disabled="!property.checked" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button type="delete" icon="minus" size="mini" v-if="!property.isDefault" @click.native="removeJobProperty(index)"></el-button>
    </el-col>
  </el-row>
  <div class="line"></div>
    <el-row>
    <el-col :span="12">
      <div class="cube-config">
        <h2>{{$t('hiveRelated')}}</h2>
        <el-button type="default" icon="plus" @click.native="addNewHiveProperty">
        </el-button>
      </div>
    </el-col>
  </el-row>
  <el-row v-for="(property, index) in hiveConvertedProperties" :key="index" :gutter="20" class="row_padding">
    <el-col :span="7">
      <el-input   :disabled="true" placeholder="kylin.hive.config.override">
      </el-input>
    </el-col>
    <el-col :span="7">
      <el-input  v-model="property.key" :disabled="property.isDefault" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="7">
      <el-input  v-model="property.value" :disabled="!property.checked" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button type="delete" icon="minus" size="mini" v-if="!property.isDefault" @click.native="removeHiveProperty(index)"></el-button>
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
      this.convertedProperties.push({checked: true, key: '', value: '', isDefault: false})
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
      this.jobConvertedProperties.push({checked: true, key: '', value: '', isDefault: false})
      this.$set(this.cubeDesc.override_kylin_properties, '', '')
    },
    removeJobProperty: function (index) {
      this.$delete(this.cubeDesc.override_kylin_properties, this.jobConvertedProperties[index].key)
      this.jobConvertedProperties.splice(index, 1)
    },
    addNewHiveProperty: function () {
      this.hiveConvertedProperties.push({checked: true, key: '', value: '', isDefault: false})
      this.$set(this.cubeDesc.override_kylin_properties, '', '')
    },
    removeHiveProperty: function (index) {
      this.$delete(this.cubeDesc.override_kylin_properties, this.hiveConvertedProperties[index].key)
      this.hiveConvertedProperties.splice(index, 1)
    },
    initProperty: function () {
      let defaultConfigs = fromObjToArr(this.$store.state.config.defaultConfig)
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
    this.loadConfig().then(() => {
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
    'en': {tip: 'Tips', propertyTip: 'Cube level properties will overwrite configuration in kylin.properties', addConfiguration: 'Add Configuration', engineTip: 'Select cube engine for building cube.', cubeDefault: 'Cube default configuration', jobRelated: 'Job related configuration', hiveRelated: 'Hive related configuration'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.properties中的属性值', addConfiguration: '添加配置', engineTip: '选择一个Cube构建引擎。', cubeDefault: 'Cube 默认配置', jobRelated: 'Job 相关配置', hiveRelated: 'Hive 相关配置'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .row_padding {
    padding-top: 5px;
    padding-bottom: 5px;
  }
  #overwrites{
    .el-checkbox__inner{
      margin-top: 10px;
    }
    .cube-config {
      padding-bottom: 10px;
      h2 {
        float:left;
        line-height: 250%
      }
      .el-button--default{
        margin-left: 15px;
        border-color: @base-color!important;
      }
    }
    .el-button--mini{
      margin-top: 5px;
      border-color: rgb(255,73,73);
    }
  }
</style>
