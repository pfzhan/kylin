<template>
  <el-dialog class="reload-modal"
    width="660px"
    :title="$t('reload')"
    :visible="isShow"
    @close="hideModal">
    <div class="tree_check_content ksd-mt-20">
      <div class="ksd-mt-20 ksd-mb-40">
          <slider @changeBar="changeBar" :show="isShow">
            <span slot="checkLabel">{{$t('sampling')}}</span>
            <span slot="tipLabel">
              <common-tip :content="$t('kylinLang.dataSource.collectStatice')" >
                <i class="el-icon-question"></i>
            </common-tip>
            </span>
          </slider>
      </div>
    </div>

    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="hideModal">{{$t('cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import { handleSuccessAsync } from '../../../../util'

@Component({
  props: {
    table: {
      type: Object
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getTableJob: 'GET_TABLE_JOB'
    })
  }
})
export default class ReloadModal extends Vue {
  isShow = false
  tableStaticsRange = 100
  openCollectRange = false

  async showModal () {
    const tableName = `${this.table.database}.${this.table.name}`
    this.tableStaticsRange = 0
    this.openCollectRange = false

    if (await this.checkTableHasJob(tableName)) {
      this.$message(this.$t('kylinLang.dataSource.dataSourceHasJob'))
    } else {
      this.isShow = true
    }
  }
  hideModal () {
    this.isShow = false
  }
  changeBar (val) {
    this.tableStaticsRange = val
    this.openCollectRange = !!val
  }
  async submit () {
    // const tableName = `${this.table.database}.${this.table.name}`
    // const res = await this.loadHiveInProject({
    //   project: this.currentSelectedProject,
    //   data: {
    //     ratio: (this.tableStaticsRange / 100).toFixed(2),
    //     tables: [tableName],
    //     project: this.currentSelectedProject,
    //     needProfile: this.openCollectRange
    //   }
    // })
    // this.datasource = await handleSuccessAsync(res)
  }
  async checkTableHasJob (tableName) {
    const res = await this.getTableJob({tableName: tableName, project: this.currentSelectedProject})
    const data = await handleSuccessAsync(res)
    return !(data && (data.job_status === 'FINISHED' || String(data.progress) === '100' || data.job_status === 'DISCARDED') || !data)
  }
}
</script>

<style lang="less">
</style>
