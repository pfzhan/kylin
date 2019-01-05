<template>
  <div class="source-management">
    <h1 class="title">{{$t('sourceManagement')}}</h1>
    <el-table border :data="sourceArray">
      <el-table-column prop="name" :label="$t('name')" width="530"></el-table-column>
      <el-table-column prop="type" :label="$t('type')"></el-table-column>
      <el-table-column prop="createTime" :label="$t('createTime')">
        <template slot-scope="scope">
          <span>{{scope.row.createTime | gmtTime}}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.common.action')">
        <template slot-scope="scope">
          <i class="el-icon-ksd-batch_check" @click="handleBatchLoad"></i>
          <el-dropdown trigger="click">
            <i class="el-icon-ksd-table_others"></i>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="() => {}">{{$t('general')}}</el-dropdown-item>
              <el-dropdown-item @click.native="() => {}">{{$t('removeSource')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleError } from '../../../../util'

@Component({
  props: {
    project: {
      type: Object
    }
  },
  methods: {
    ...mapActions('BatchLoadModal', {
      callBatchLoadModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class SourceManagement extends Vue {
  sourceArray = []
  tableArray = []
  isBatchLoadShow = false
  async mounted () {
    try {
      await this.loadSource()
    } catch (e) {
      handleError(e)
    }
  }
  loadSource () {
    this.sourceArray.push({ name: 'Default', type: 'Hive', createTime: new Date().getTime() })
  }
  async handleBatchLoad () {
    const { project } = this
    const isSubmit = await this.callBatchLoadModal({ project })
    isSubmit && this.$emit('fresh-tables')
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.source-management {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  background: @fff;
  z-index: 3;
  padding: 20px;
  .title {
    font-size: 14px;
    color: @text-title-color;
    margin-bottom: 10px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
}
</style>
