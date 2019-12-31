<template>
  <div>
    <!-- 搜索区 -->
    <p class="clearfix">
      <el-input
        class="ksd-fright"
        :placeholder="$t('kylinLang.common.pleaseFilter')"
        prefix-icon="el-icon-search"
        v-model.trim="keyword"
        @keyup.native="handleFilter()"
        @clear="handleFilter()"
        style="width:300px">
      </el-input>
    </p>
    
    <!-- 实际信息展示区 -->
    <el-table
      key="measureList"
      ref="measureList"
      border
      :empty-text="emptyText"
      class="ksd-mt-10 measure-table"
      :data="tableShowList"
      style="width: 100%"
      @select-all="selectionAllChange"
      @select="selectionChange">
      <el-table-column
        type="selection"
        width="44">
      </el-table-column>
      <el-table-column
        width="100"
        :renderHeader="renderRecommendType"
        prop="recommendation_type">
        <template slot-scope="scope">
          {{$t(scope.row.recommendation_type)}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('th_name')"
        width="180">
        <template slot-scope="scope">
          <div>
            <el-input size="small" v-model.trim="scope.row.measure.name" @change="changeColumnName(scope.row)" :disabled="!scope.row.isSelected"></el-input>
            <div v-if="scope.row.validateNameRule" class="ky-form-error">{{$t('kylinLang.common.nameFormatValidTip')}}</div>
                      <div v-else-if="scope.row.validateSameName" class="ky-form-error">{{$t('kylinLang.common.sameName')}}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('th_column')"
        show-overflow-tooltip>
        <template slot-scope="scope">
          <div v-if="scope.row.measure.function.expression === 'TOP_N'">
            <p>Order / Sum by: {{scope.row.measure.function.column}}</p>
            <p>Group By: {{dealParameters(scope.row.measure.function.parameters)}}</p>
          </div>
          <div v-else>
            {{dealParameters(scope.row.measure.function.parameters)}}
          </div>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('th_function')"
        show-overflow-tooltip
        width="140">
        <template slot-scope="scope">
          {{scope.row.measure.function.expression}}
        </template>
      </el-table-column>
      <el-table-column
        prop="parameter"
        :label="$t('th_parameter')"
        width="160">
        <template slot-scope="scope">
          <el-select
            size="mini"
            v-model="scope.row.measure.function.returntype"
            :placeholder="$t('kylinLang.common.pleaseSelect')"
            v-if="scope.row.measure.function.expression === 'TOP_N'|| scope.row.measure.function.expression === 'PERCENTILE_APPROX' || scope.row.measure.function.expression === 'COUNT_DISTINCT'">
            <el-option
              v-for="item in getSelectDataType(scope.row.measure.function.expression)"
              :key="item.value"
              :label="item.name"
              :value="item.value">
            </el-option>
          </el-select>
          <p v-else>{{scope.row.measure.function.returntype}}</p>
        </template>
      </el-table-column>
      <el-table-column
        width="180"
        prop="create_time"
        :label="$t('th_updateDate')"
        sortable
        show-overflow-tooltip>
        <template slot-scope="scope">
          {{transToGmtTime(scope.row.create_time)}}
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import { checkNameRegex, renderTableColumnSelected } from '../handler'
  // import vuex from '../../../../store'
  import { transToGmtTime } from 'util/business'
  import { objectClone } from 'util/index'
  import locales from '../locales'
  
  @Component({
    props: {
      list: {
        type: Array,
        default: () => []
      }
    },
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState({
      })
    },
    methods: {
      transToGmtTime: transToGmtTime,
      ...mapActions({
      }),
      ...mapMutations({
      })
    },
    locales
  })
  export default class recomMeasure extends Vue {
    keyword = ''
    checkedStatus = []
    tableShowList = []
    ST = null
    measureValidPass = true // 判断表单校验是否通过
    topNTypes = [
      {name: 'Top 10', value: 'topn(10)'},
      {name: 'Top 100', value: 'topn(100)'},
      {name: 'Top 1000', value: 'topn(1000)'}
    ]
    distinctDataTypes = [
      {name: 'Error Rate < 9.75%', value: 'hllc(10)'},
      {name: 'Error Rate < 4.88%', value: 'hllc(12)'},
      {name: 'Error Rate < 2.44%', value: 'hllc(14)'},
      {name: 'Error Rate < 1.72%', value: 'hllc(15)'},
      {name: 'Error Rate < 1.22%', value: 'hllc(16)'},
      {name: 'Precisely', value: 'bitmap'}
    ]
    percentileTypes = [
      {name: 'percentile(100)', value: 'percentile(100)'},
      {name: 'percentile(1000)', value: 'percentile(1000)'},
      {name: 'percentile(10000)', value: 'percentile(10000)'}
    ]

    get emptyText () {
      return this.keyword ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
    }

    getSelectDataType (expression) {
      if (expression === 'TOP_N') {
        return this.topNTypes
      }
      if (expression === 'COUNT_DISTINCT') {
        return this.distinctDataTypes
      }
      if (expression === 'PERCENTILE_APPROX') {
        return this.percentileTypes
      }
    }

    dealParameters (parameters) {
      let temp = parameters.map((item) => {
        return item.value
      })
      return temp.join(',')
    }

    /* @Watch('list', { deep: true })
    onListChange (val) {
      // this.tableShowList = objectClone(val)
      this.tableShowList = val
      renderTableColumnSelected(this.tableShowList, this, 'measureList')
    } */

    mounted () {
      this.tableShowList = this.list
      renderTableColumnSelected(this.tableShowList, this, 'measureList')
    }

    // 检测是否有重名
    checkNameColumn (curColumn) {
      // 再检查是否同名
      for (let i = 0; i < this.list.length; i++) {
        let column = this.list[i]
        // 先检测名字是否合规
        if (!checkNameRegex(column.measure.name)) {
          this.$set(this.list[i], 'validateNameRule', true)
        } else {
          this.$set(this.list[i], 'validateNameRule', false)
        }
        // 再检查是否同名
        if (curColumn.measure.name === column.measure.name && curColumn.item_id !== column.item_id) {
          this.$set(this.list[i], 'validateSameName', true)
        } else {
          this.$set(this.list[i], 'validateSameName', false)
        }
      }
    }

    // 修改每个度量名时触发
    changeColumnName (item) {
      // 要检查修改的名字是否ok
      this.checkNameColumn(item)
      let hasInValidColumnArr = this.list.filter((column) => {
        return column.validateSameName || column.validateNameRule
      })
      item.validateSameName = hasInValidColumnArr.length > 0
      this.measureValidPass = hasInValidColumnArr.length === 0
    }

    // 通知外部，选中/取消选中所有的行，更变list的isSelect的值
    selectionAllChange (selection) {
      let selectAllType = selection.length > 0 ? 'all' : 'null'
      this.$emit('measureSelectedChange', {type: 'measure', selectType: selectAllType})
    }

    // 单元行的选中/取消
    selectionChange (selection, row) {
      row.isSelected = !row.isSelected
      this.$emit('measureSelectedChange', {type: 'measure', selectType: 'single', data: row})
    }

    handleFilter () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        let keywordStr = this.keyword.toLocaleLowerCase()
        this.tableShowList = this.list.filter((item) => {
          let typeIn = item.recommendation_type.toLocaleLowerCase().indexOf(keywordStr) > -1
          let nameIn = item.measure.name.toLocaleLowerCase().indexOf(keywordStr) > -1
          let columnIn = item.measure.column.indexOf(keywordStr) > -1
          let funIn = item.measure.function.expression.indexOf(keywordStr) > -1
          // todo 函数参数还没判断
          let hasKeyword = typeIn || nameIn || columnIn || funIn
          return hasKeyword
        })
        renderTableColumnSelected(this.tableShowList, this, 'measureList')
      }, 100)
    }

    filterFav () { // 筛选建议类型
      if (this.checkedStatus.length === 0) {
        this.tableShowList = objectClone(this.list)
      } else {
        this.tableShowList = this.list.filter((item) => {
          return this.checkedStatus.indexOf(item.recommendation_type) > -1
        })
      }
      renderTableColumnSelected(this.tableShowList, this, 'measureList')
    }

    renderRecommendType (h) {
      let typeList = ['ADDITION', 'REMOVAL']
      let items = []
      for (let i = 0; i < typeList.length; i++) {
        items.push(<el-checkbox label={typeList[i]} key={typeList[i]}><span class="ksd-fs-12">{this.$t(typeList[i])}</span></el-checkbox>)
      }
      return (<span>
        <span>{this.$t('th_recommendType')}</span>
        <el-popover
          ref="ipFilterPopover"
          placement="bottom"
          popperClass="filter-popover">
          <el-checkbox-group class="filter-groups" value={this.checkedStatus} onInput={val => (this.checkedStatus = val)} onChange={this.filterFav}>
            {items}
          </el-checkbox-group>
          <i class={this.checkedStatus.length > 0 && this.checkedStatus.length < 3 ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
        </el-popover>
      </span>)
    }
  }
</script>
<style lang="less">
  @import '../../../../../../assets/styles/variables.less';
  .measure-table {
    .el-icon-ksd-filter {
      &:hover {
        color: @base-color;
      }
      &.isFilter {
        color: @base-color;
      }
    }
  }
</style>
