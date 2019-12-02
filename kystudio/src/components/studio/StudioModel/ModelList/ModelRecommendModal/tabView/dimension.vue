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
      key="dimensionList"
      ref="dimensionList"
      border
      class="ksd-mt-10"
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
        prop="recommendation_type"
        :renderHeader="renderRecommendType">
        <template slot-scope="scope">
          {{$t(scope.row.recommendation_type)}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('th_name')">
        <template slot-scope="scope">
          <div>
            <el-input size="small" v-model.trim="scope.row.column.name" @change="changeColumnName(scope.row)" :disabled="!scope.row.isSelected"></el-input>
            <div v-if="scope.row.validateNameRule" class="ky-form-error">{{$t('kylinLang.common.nameFormatValidTip')}}</div>
            <div v-else-if="scope.row.validateSameName" class="ky-form-error">{{$t('kylinLang.common.sameName')}}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('th_column')"
        show-overflow-tooltip>
        <template slot-scope="scope">
          {{scope.row.column.column}}
        </template>
      </el-table-column>
      <el-table-column
        width="100"
        prop="data_type"
        :label="$t('th_dataType')"
        show-overflow-tooltip>
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
  export default class recomDimension extends Vue {
    keyword = ''
    checkedStatus = []
    tableShowList = []
    ST = null
    dimensionValidPass = true // 判断表单校验是否通过

    /* @Watch('list', { deep: true })
    onListChange (val) {
      // this.tableShowList = objectClone(val)
      console.log(val)
      this.tableShowList = val
      renderTableColumnSelected(this.tableShowList, this, 'dimensionList')
    } */

    mounted () {
      this.tableShowList = this.list
      renderTableColumnSelected(this.tableShowList, this, 'dimensionList')
    }

    // 检测是否有重名
    checkNameColumn (curColumn) {
      for (let i = 0; i < this.list.length; i++) {
        let column = this.list[i]
        // 先检测名字是否合规
        if (!checkNameRegex(column.column.name)) {
          this.$set(this.list[i], 'validateNameRule', true)
        } else {
          this.$set(this.list[i], 'validateNameRule', false)
        }
        // 再检查是否同名
        if (curColumn.column.name === column.column.name && curColumn.item_id !== column.item_id) {
          this.$set(this.list[i], 'validateSameName', true)
        } else {
          this.$set(this.list[i], 'validateSameName', false)
        }
      }
    }

    // 修改每个维度名时触发
    changeColumnName (item) {
      // 要检查修改的名字是否ok
      this.checkNameColumn(item)
      let hasInValidColumnArr = this.list.filter((column) => {
        return column.validateSameName || column.validateNameRule
      })
      item.validateSameName = hasInValidColumnArr.length > 0
      this.dimensionValidPass = hasInValidColumnArr.length === 0
    }

    // 通知外部，选中/取消选中所有的行，更变list的isSelect的值
    selectionAllChange (selection) {
      let selectAllType = selection.length > 0 ? 'all' : 'null'
      this.$emit('dimensionSelectedChange', {type: 'dimension', selectType: selectAllType})
    }

    // 单元行的选中/取消
    selectionChange (selection, row) {
      row.isSelected = !row.isSelected
      this.$emit('dimensionSelectedChange', {type: 'dimension', selectType: 'single', data: row})
    }

    handleFilter () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        let keywordStr = this.keyword.toLocaleLowerCase()
        this.tableShowList = this.list.filter((item) => {
          let typeIn = item.recommendation_type.toLocaleLowerCase().indexOf(keywordStr) > -1
          let dataTypeIn = item.data_type.toLocaleLowerCase().indexOf(keywordStr) > -1
          let nameIn = item.column.name.toLocaleLowerCase().indexOf(keywordStr) > -1
          let columnIn = item.column.column.indexOf(keywordStr) > -1
          let hasKeyword = typeIn || dataTypeIn || nameIn || columnIn
          return hasKeyword
        })
        renderTableColumnSelected(this.tableShowList, this, 'dimensionList')
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
      renderTableColumnSelected(this.tableShowList, this, 'dimensionList')
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
</style>
