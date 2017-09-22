<template>
	<tree :treedata="modelAssets" :placeholder="this.$t('kylinLang.common.pleaseFilter')" maxLabelLen="20" showfilter= "true" :expandall="true" v-on:treedrag="drag" allowdrag=true  v-unselect maxlevel="3"></tree>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess } from '../../util/business'
export default {
  data () {
    return {
      modelAssets: []
    }
  },
  props: ['project'],
  methods: {
    ...mapActions({
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    }),
    drag (target, data) {
      this.$emit('drag', target, data.id)
    }
  },
  created () {
    var _this = this
    this.loadDataSourceByProject({project: this.project, isExt: false}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        var datasourceData = data
        var datasourceTreeData = {
          id: 1,
          label: 'Data Source',
          children: []
        }
        var databaseData = {}
        for (var i = 0; i < datasourceData.length; i++) {
          databaseData[datasourceData[i].database] = databaseData[datasourceData[i].database] || []
          databaseData[datasourceData[i].database].push(datasourceData[i])
        }
        for (var s in databaseData) {
          var obj = {}
          obj.id = s
          obj.label = s
          obj.children = []
          for (var f = 0; f < databaseData[s].length; f++) {
            var childObj = {}
            childObj.id = s + '$' + databaseData[s][f].name
            childObj.label = databaseData[s][f].name
            childObj.icon = databaseData[s][f].source_type === 1 ? 'streaming' : 'normal'
            obj.children.push(childObj)
          }
          datasourceTreeData.children.push(obj)
        }
        _this.modelAssets = [datasourceTreeData]
      })
    })
  },
  mounted () {
    this.$emit('okFunc')
  }
}
</script>
<style>
	
</style>
