<template>
<div class="box-card" id="overwrites">
  <div v-show="!isPlusVersion">
  <h2>Cubing Engine <common-tip :content="$t('engineTip')" ><icon name="question-circle-o"></icon></common-tip></h2>
  <p class="ksd-mt-14">Engine Type: 
    <el-select :disabled="isPlusVersion" v-model="cubeDesc.engine_type">
      <el-option  v-for="item in engineType"
      :key="item.value"
      :label="item.name"
      :value="item.value"></el-option>
    </el-select>
  </p>
  </div>
  <div class="line" v-show="!isPlusVersion"></div>
  <el-row>
    <el-col :span="24" style="font-size: 14px;margin-bottom: 10px;">  {{$t('propertyTip')}}</el-col>
      </el-row>
  </el-row>
  <el-row v-for="(property, index) in convertedProperties" :key="index" :gutter="20" class="row_padding">
    <el-col :span="1">
      <el-checkbox v-show="property.isDefault" v-model="property.checked" @change="changeProperty(index)"></el-checkbox>
    </el-col>
     <el-col :span="10" :offset="property.isDefault?0:1">
      <el-input  v-model="property.key" :disabled="property.isDefault" @change="changeProperty(index)"></el-input> 
    </el-col>
    <el-col :span="10">
      <el-input  v-model="property.value" :disabled="!property.checked" @change="changeProperty(index)"></el-input>
    </el-col>
    <el-col :span="1">
      <el-button type="delete" icon="minus" size="mini" v-if="!property.isDefault" @click.native="removeProperty(index)"></el-button>
    </el-col>        
  </el-row>
  <el-row>
    <el-col :span="4" :offset="1">
      <el-button style="margin-top: 10px;" type="default" icon="plus" @click.native="addNewProperty">{{$t('addConfiguration')}}</el-button>
    </el-col>
  </el-row>
</div>
</template>

<script>
import { fromObjToArr } from '../../../util/index'
import { mapActions } from 'vuex'
import { engineType } from '../../../config/index'
export default {
  name: 'configurationOverwrites',
  props: ['cubeDesc'],
  data () {
    return {
      convertedProperties: [],
      properties: fromObjToArr(this.cubeDesc.override_kylin_properties),
      engineType: engineType
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
    changeProperty: function (index) {
      this.cubeDesc.override_kylin_properties = {}
      this.convertedProperties.forEach((item, index) => {
        if (item.checked) {
          this.$set(this.cubeDesc.override_kylin_properties, this.convertedProperties[index].key, this.convertedProperties[index].value)
        }
      })
    },
    initProperty: function () {
      let _this = this
      let defaultConfigs = fromObjToArr(_this.$store.state.config.defaultConfig)
      defaultConfigs.forEach(function (config) {
        _this.convertedProperties.push({checked: false, key: config.key, value: config.value, isDefault: true})
      })
      for (let key of Object.keys(_this.cubeDesc.override_kylin_properties)) {
        let isDefault = false
        _this.convertedProperties.forEach(function (property, index) {
          if (key === property.key) {
            _this.$set(property, 'value', _this.cubeDesc.override_kylin_properties[key])
            _this.$set(property, 'checked', true)
            _this.$set(property, 'isDefault', true)
            isDefault = true
          }
        })
        if (!isDefault) {
          _this.convertedProperties.push({checked: true, key: key, value: _this.cubeDesc.override_kylin_properties[key], isDefault: false})
        }
      }
    }
  },
  created: function () {
    let _this = this
    this.loadConfig().then(() => {
      _this.initProperty()
    })
  },
  computed: {
    isPlusVersion () {
      var kapVersionInfo = this.$store.state.system.serverAboutKap
      return kapVersionInfo && kapVersionInfo['kap.version'] && kapVersionInfo['kap.version'].indexOf('Plus') !== -1
    }
  },
  locales: {
    'en': {tip: 'Tip', propertyTip: 'Cube level properties will overwrite configuration in kylin.properties', addConfiguration: 'Add Configuration', 'engineTip': 'Select cube engine for building cube.'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.properties中的属性值', addConfiguration: '添加配置', 'engineTip': '选择一个Cube构建引擎。'}
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
    .el-button--primary:hover{
      border-color: @base-color;
    }
    .el-button--mini{
      margin-top: 5px;
    }
  }
</style>
