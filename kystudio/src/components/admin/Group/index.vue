<template>
  <div class="security-group">
    <div class="ksd-title-label ksd-mt-20 ksd-mrl-20">{{$t('userGroupsList')}}</div>
    <el-row class="ksd-mb-10 ksd-mt-10 ksd-mrl-20">
      <el-button type="primary" plain size="medium" v-if="groupActions.includes('addGroup')" icon="el-icon-ksd-add_2" @click="editGroup('new')">{{$t('kylinLang.common.group')}}</el-button>
      <div style="width:200px;" class="ksd-fright">
        <el-input class="show-search-btn"
          size="medium"
          prefix-icon="el-icon-search"
          :placeholder="$t('groupFilter')"
          v-global-key-event.enter.debounce="inputFilter"
          @clear="inputFilter('')"
        >
        </el-input>
      </div>
    </el-row>
    <el-row class="ksd-mrl-20">
      <el-table
        :data="groupUsersList"
        class="group-table"
        :empty-text="emptyText"
        v-scroll-shadow
        border>
        <el-table-column
          :label="$t('kylinLang.common.name')"
          show-overflow-tooltip
          prop="first">
          <template slot-scope="scope">
            <i class="el-icon-ksd-table_group ksd-fs-14" style="cursor: default;"></i>
            <router-link :to="{path: '/admin/group/' + scope.row.first}" class="group-name">{{scope.row.first}}</router-link>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('usersCount')"
          prop="second"
          show-overflow-tooltip>
          <template slot-scope="scope">
            {{scope.row.second && scope.row.second.length || 0}}
          </template>
        </el-table-column>
        <el-table-column v-if="groupActions.includes('editGroup') && groupActions.includes('deleteGroup')"
          :label="$t('kylinLang.common.action')" :width="83">
          <template slot-scope="scope">
            <el-tooltip :content="$t('assignUsers')" effect="dark" placement="top" v-show="scope.row.first!=='ALL_USERS' && groupActions.includes('editGroup')">
              <i class="el-icon-ksd-table_assign ksd-fs-14 ksd-mr-10" @click="editGroup('assign', scope.row)"></i>
            </el-tooltip><span>
            </span><el-tooltip :content="$t('kylinLang.common.drop')" effect="dark" placement="top" v-show="(scope.row.first!=='ROLE_ADMIN' && scope.row.first!=='ALL_USERS') && groupActions.includes('deleteGroup')">
              <i class="el-icon-ksd-table_delete ksd-fs-14" @click="dropGroup(scope.row.first)"></i>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>

      <kap-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :totalSize="groupUsersListSize"
        :curPage="pagination.page_offset+1"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </el-row>

    <GroupEditModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapGetters, mapActions } from 'vuex'

import locales from './locales'
import { pageCount } from 'config'
import GroupEditModal from '../../common/GroupEditModal/index.vue'
import { handleError, kapConfirm } from 'util/business'

@Component({
  components: {
    GroupEditModal
  },
  computed: {
    ...mapGetters([
      'groupActions',
      'currentSelectedProject'
    ]),
    ...mapState({
      groupUsersListSize: state => state.user.usersGroupSize,
      userListData: state => state.user.usersList,
      groupUsersList: state => state.user.usersGroupList
    })
  },
  methods: {
    ...mapActions({
      loadGroupUsersList: 'GET_GROUP_USERS_LIST',
      delGroup: 'DEL_GROUP',
      loadUsersList: 'LOAD_USERS_LIST'
    }),
    ...mapActions('GroupEditModal', {
      callGroupEditModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class SecurityGroup extends Vue {
  pagination = {
    page_size: pageCount,
    page_offset: 0
  }
  filterName = ''

  created () {
    this.loadGroupUsers()
    if (this.groupActions.includes('viewGroup')) {
      this.loadUsers().then((res) => {
        if (res.status !== 200) {
          handleError(res)
        }
      })
    }
  }

  async editGroup (editType, group) {
    const isSubmit = await this.callGroupEditModal({ editType, group })
    isSubmit && this.loadGroupUsers()
  }

  inputFilter (value) {
    this.pagination.page_offset = 0
    this.filterName = value
    this.loadGroupUsers(value)
  }

  get emptyText () {
    return this.filterName ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  loadUsers (filterName) {
    return this.loadUsersList({
      ...this.pagination,
      name: filterName,
      project: this.currentSelectedProject
    })
  }

  async dropGroup (groupName) {
    try {
      await kapConfirm(this.$t('confirmDelGroup', {groupName: groupName}), null, this.$t('delGroupTitle'))
      await this.delGroup({group_name: groupName})
      await this.loadGroupUsers()
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }

  loadGroupUsers (filterGroupName) {
    this.loadGroupUsersList({
      ...this.pagination,
      user_group_name: filterGroupName
    }).then(() => {
    }, (res) => {
      handleError(res)
    })
  }

  handleCurrentChange (pager, pageSize) {
    this.pagination.page_offset = pager
    this.pagination.page_size = pageSize
    this.loadGroupUsers()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.security-group {
  .el-icon-ksd-table_group {
    color: @base-color-1;
  }
  .group-table {
    .el-icon-ksd-table_assign:hover,
    .el-icon-ksd-table_delete:hover {
      color: @base-color;
    }
    .group-name {
      white-space: pre;
    }
  }
}
</style>
