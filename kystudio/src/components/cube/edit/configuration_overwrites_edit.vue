<template>
<el-card class="box-card">
  <el-row>
    <el-col :span="18">
      <el-row v-for="property in properties">
        <el-col :span="12">{{property.key}}</el-col>
        <el-col :span="12">{{property.value}}</el-col>
      </el-row>
      <el-button type="primary" icon="plus" @click.native="addNewProperty"></el-button>
    </el-col>
    <el-col :span="6">{{$t('tip')}}
      <el-row >
        <el-col :span="24">{{$t('propertyTip')}}</el-col>
      </el-row>
    </el-col>
  </el-row>
</el-card>
</template>

<script>
import { fromObjToArr } from '../../../util/index'
import { mapActions } from 'vuex'
export default {
  name: 'configurationOverwrites',
  props: ['cubeDesc'],
  data () {
    return {
      properties: fromObjToArr(this.cubeDesc.override_kylin_properties)
    }
  },
  methods: {
    ...mapActions({
      loadConfig: 'LOAD_DEFAULT_CONFIG'
    })
  },
  created: function () {
    this.loadConfig()
  },
  computed: {
    getDefaultConfig: function () {
      return this.$store.state.config.defaultConfig
    }
  },
  locales: {
    'en': {tip: 'Tip', propertyTip: 'Cube level properties will overwrite configuration in kylin.prperties'},
    'zh-cn': {tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.prperties中的属性值'}
  }
}
</script>
<style scoped="">

</style>
