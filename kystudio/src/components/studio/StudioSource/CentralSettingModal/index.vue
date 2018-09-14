<template>
  <el-dialog class="central-setting-modal"
    width="660px"
    :title="$t('centralSetting')"
    :visible="isShow"
    @close="hideModal">
    <div class="body">
      <div class="row">
        <h1 class="title font-medium">
          <span>{{$t('tableType')}}</span>
          <i class="el-icon-ksd-what"></i>
        </h1>
        <el-radio v-model="isCentral" :label="true" :disabled="!partitionColumns.length">{{$t('centralTable')}}</el-radio>
        <el-radio v-model="isCentral" :label="false">{{$t('normalTable')}}</el-radio>
      </div>

      <div class="row">
        <h1 class="title font-medium">
          <span>{{$t('partition')}}</span>
          <i class="el-icon-ksd-what"></i>
        </h1>
        <el-select size="medium" v-model="partition" filterable :disabled="!isCentral">
          <el-option
            v-for="column in partitionColumns"
            :key="column.id"
            :label="column.name"
            :value="column.name">
          </el-option>
        </el-select>
      </div>

      <div class="row">
        <h1 class="title font-medium">
          <span>{{$t('dataRange')}}</span>
          <i class="el-icon-ksd-what"></i>
        </h1>
        <div>
          <label class="font-medium">
            <span>{{$t('dateRange')}}</span>
            <i class="el-icon-ksd-what"></i>
            <span>:</span>
          </label>
          <el-date-picker
            :disabled="!isDateRangeEditable"
            v-model="dateRange"
            type="datetimerange"
            size="medium"
            range-separator="-"
            :start-placeholder="$t('startTime')"
            :end-placeholder="$t('endTime')">
          </el-date-picker>
        </div>
      </div>
    </div>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="hideModal">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="submit" :disabled="!isFormVaild">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleError } from '../../../../util/business'
import { partitionColumnTypes } from '../../../../config'

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
      saveFactTable: 'SAVE_FACT_TABLE',
      saveDateRange: 'SAVE_DATE_RANGE'
    })
  },
  locales
})
export default class CentralSettingModal extends Vue {
  isShow = false
  isCentral = false
  partition = ''
  startDate = ''
  endDate = ''
  get dateRange () {
    return [this.startDate, this.endDate]
  }
  set dateRange ([startDate, endDate]) {
    this.startDate = startDate
    this.endDate = endDate
  }
  get partitionColumns () {
    return this.table.columns.filter(column => partitionColumnTypes.includes(column.datatype))
  }
  get isFormVaild () {
    return (this.isCentral && this.partition && this.startDate && this.endDate) || !this.isCentral
  }
  get isDateRangeEditable () {
    return !this.table.fact && this.isCentral
  }
  showModal () {
    this.resetModal()
    this.isShow = true
  }
  hideModal () {
    this.isShow = false
  }
  resetModal () {
    this.isCentral = this.table.fact || false
    this.partition = this.table.partition_column || ''
    this.startDate = this.table.start_time !== -1 ? new Date(this.table.start_time) : ''
    this.endDate = this.table.end_time !== -1 ? new Date(this.table.end_time) : ''
  }
  async submit () {
    try {
      const { startDate, endDate, table, isCentral, partition } = this
      const tableFullName = `${table.database}.${table.name}`

      await this.saveFactTable({
        projectName: this.currentSelectedProject,
        tableFullName,
        isCentral,
        column: partition !== '' ? partition : undefined
      })

      await this.saveDateRange({
        projectName: this.currentSelectedProject,
        tableFullName,
        startDate: startDate && startDate.getTime(),
        endDate: endDate && endDate.getTime()
      })

      this.hideModal()
      this.$emit('submit')
    } catch (e) {
      handleError(e)
    }
  }
}
</script>

<style lang="less">
.central-setting-modal {
  .title {
    font-size: 16px;
    margin-bottom: 10px;
  }
  .row {
    margin-bottom: 20px;
    &:last-child {
      margin-bottom: 0;
    }
  }
  .el-date-editor {
    // width: 340px;
  }
  .el-date-editor .el-flex-box {
    width: 100%;
  }
}
</style>
