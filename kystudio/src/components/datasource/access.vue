<template>
    <div class="access_menu">
        <el-tabs v-model="currentTab" type="card" @tab-click="handleClick">
          <el-tab-pane :label="$t('table')" name="table">
            <table-access v-if="currentTab === 'table'"></table-access>
          </el-tab-pane>
          <el-tab-pane :label="$t('row')" name="row">
            <row-access v-if="currentTab === 'row'"></row-access>
          </el-tab-pane>
          <el-tab-pane :label="$t('column')" name="column">
             <column-access v-if="currentTab === 'column'"></column-access>
          </el-tab-pane>
        </el-tabs>
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
import tableAccess from './access_sub/table_access'
import columnAccess from './access_sub/column_access'
import rowAccess from './access_sub/row_access'
export default {
  data () {
    return {
      currentTab: 'table'
    }
  },
  components: {
    'table-access': tableAccess,
    'column-access': columnAccess,
    'row-access': rowAccess
  },
  created () {
  },
  methods: {
    ...mapActions({
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    }),
    handleClick (tab, event) {
      // console.log(tab, event)
    }
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
    'en': {'table': 'Table', 'row': 'Row', 'column': 'Column'},
    'zh-cn': {'table': '表', 'row': '行', 'column': '列'}
  }
}
</script>
<style lang="less" >
// @import '../../assets/styles/variables.less';
// .access_menu{
//   &>.el-tabs{
//     &>.el-tabs__header{
//       margin-left:0;
//       &>.el-tabs__nav-wrap{
//         &>.el-tabs__nav-scroll{
//           &>.el-tabs__nav{
//             &>.el-tabs__item{
//               &.is-active{
//                 background-color: #393e53!important;
//                 color:@base-color;
//               }
//             }
//           }
//         }
//       }
//     }
//   }
// }
</style>
