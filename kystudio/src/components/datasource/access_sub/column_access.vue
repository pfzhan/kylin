<template>
    <div class="access" id="datasource">
       
    </div>
</template>
<script>
import { mapActions } from 'vuex'
// import { handleSuccess, handleError, hasRole, kapWarn, transToGmtTime, hasPermission } from '../../util/business'
// import { permissions } from '../../config'
// import { changeDataAxis, isFireFox } from '../../util/index'
// import createKafka from '../kafka/create_kafka'
// import editKafka from '../kafka/edit_kafka'
// import viewKafka from '../kafka/view_kafka'
// import arealabel from 'components/common/area_label'
// import Scrollbar from 'smooth-scrollbar'
export default {
  data () {
    return {}
  },
  components: {
  },
  created () {
  },
  methods: {
    ...mapActions({
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    })
  },
  computed: {
    isAdmin () {
      // return hasRole(this, 'ROLE_ADMIN')
    }
  },
  watch: {
  },
  mounted () {
  },
  locales: {
    'en': {'load': 'Load', 'reload': 'Reload', 'samplingBtn': 'Sampling', 'sampling': 'Table Sampling', 'unload': 'Unload', 'loadhiveTables': 'Load Hive Table Metadata', 'selectLeftHiveTip': 'Please select tables from the left hive table tree', 'setScanRange': 'Table Sampling', 'filterInputTips': 'Please input the hive table name to filter', 'loadTableJobBeginTips': 'Collect job start running!You can go to Monitor page to watch the progress!', 'hasCollectJob': 'There has been a running collect job!You can go to Monitor page to watch the progress!', 'loadHiveTip': 'You can select tables from the left hive table tree or edit it manually, and you can press ENTER to distinguish table name. By default, system will choose "default" as database name, and you can specify database as \'database.table\'. Table names should be separated with comma. You can load 1000 tables once as maximum.'},
    'zh-cn': {'load': '加载', 'reload': '重载', 'samplingBtn': '采样', 'sampling': '收集表信息', 'unload': '卸载', 'loadhiveTables': '加载Hive表元数据', 'selectLeftHiveTip': '请在左侧选择要加载的table', 'setScanRange': '表采样', 'filterInputTips': '请输入hive表名进行过滤', 'loadTableJobBeginTips': '采集开始，您可以到Monitor页面查看采样进度！', 'hasCollectJob': '已有一个收集作业正在进行中，您可以去Monitor页面查看进度!', 'loadHiveTip': '您可以从左边选择要加载的表，也可以自行编辑输入，输入完成后按回车键。系统默认使用‘default’作为数据库名，您可以指定数据库名如 ‘database.table’。请使用逗号分隔表，同时最多加载1000张表。'}
  }
}
</script>
<style lang="less" >
@import '../../../less/config.less';

</style>
