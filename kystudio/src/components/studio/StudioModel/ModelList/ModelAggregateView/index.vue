<template>
  <div class="model-aggregate-view" v-loading="isLoading">
    <div class="title-list">
      <div class="title-header">
        <el-select v-model="aggType" size="small" disabled class="agg-types">
          <el-option
            v-for="item in options"
            :key="item"
            :label="$t(item)"
            :value="item">
          </el-option>
        </el-select>
        <div class="icon-group ksd-fright" v-if="isShowEditAgg">
          <common-tip :content="$t('addAggGroup')">
            <i class="el-icon-ksd-project_add" @click="handleAggregateGroup()"></i>
          </common-tip><common-tip :content="$t('aggAdvanced')">
            <i class="el-icon-ksd-setting ksd-ml-10" @click="openAggAdvancedModal()"></i>
          </common-tip>
        </div>
      </div>
      <div class="agg-total-info-block" v-if="aggregationGroups.length">
        <div>{{$t('numTitle', {num: cuboidsInfo.total_count && cuboidsInfo.total_count.result})}}</div>
        <div>{{$t('maxDimCom')}}<common-tip :content="$t('maxDimComTips')"><i class="el-icon-ksd-what ksd-mrl-2"></i></common-tip>{{$t('colon')}}
        <span v-if="aggregationObj&&!aggregationObj.global_dim_cap" class="nolimit-dim">{{$t('noLimitation')}}</span>
        <span v-if="aggregationObj&&aggregationObj.global_dim_cap" class="global-dim">{{aggregationObj.global_dim_cap}}</span>
        </div>
      </div>
      <ul v-if="aggregationGroups.length">
        <li class="agg-title" @click="scrollToMatched(aggregateIdx)" v-for="(aggregate, aggregateIdx) in aggregationGroups" :key="aggregateIdx">
          {{$t('aggregateGroupTitle', { id: aggregateIdx + 1 })}}
        </li>
      </ul>
      <kap-nodata v-else>
      </kap-nodata>
    </div>
    <div class="agg-detail-block" v-if="aggregationGroups.length">
      <div class="agg-detail ksd-mb-15" v-for="(aggregate, aggregateIdx) in aggregationGroups" :key="aggregate.id">
        <div class="agg-content-title">
          <span class="ksd-fs-14 font-medium">
            {{$t('aggregateGroupTitle', { id: aggregateIdx + 1 })}}
          </span><span class="agg-type ksd-ml-10">
            {{$t('custom')}}</span><span class="ksd-ml-15">
            {{$t('numTitle', {num: cuboidsInfo.agg_index_counts[aggregateIdx] && cuboidsInfo.agg_index_counts[aggregateIdx].result})}}
          </span><span class="divide">
          </span><span class="dimCap-block">
            <span>{{$t('maxDimCom')}}<common-tip :content="$t('dimComTips')"><i class="el-icon-ksd-what ksd-mrl-2"></i></common-tip>{{$t('colon')}}
            </span>
            <span v-if="!aggregate.select_rule.dim_cap&&aggregationObj&&!aggregationObj.global_dim_cap" class="nolimit-dim">{{$t('noLimitation')}}</span>
            <span v-if="aggregate.select_rule.dim_cap&&aggregationObj">{{aggregate.select_rule.dim_cap}}</span>
            <span v-if="!aggregate.select_rule.dim_cap&&aggregationObj&&aggregationObj.global_dim_cap" class="global-dim">{{aggregationObj.global_dim_cap}}</span>
          </span>
          <span class="ksd-fright icon-group">
            <common-tip :content="$t('kylinLang.common.edit')">
              <i class="el-icon-ksd-table_edit" @click="editAggGroup(aggregateIdx)"></i>
            </common-tip><common-tip :content="$t('kylinLang.common.delete')">
              <i class="el-icon-ksd-table_delete ksd-ml-10" @click="deleteAggGroup(aggregateIdx)"></i>
            </common-tip>
          </span>
        </div>
        <el-tabs v-model="aggregate.activeTab">
          <el-tab-pane :label="$t(item.key, {size: aggregate[item.target].length, total: totalSize(item.name)})" :name="item.key" v-for="item in tabList" :key="item.key"></el-tab-pane>
        </el-tabs>
        <template v-if="aggregate.activeTab === 'dimension'">
          <!-- Include聚合组 -->
          <div class="row ksd-mb-15">
            <div class="title font-medium ksd-mb-10">{{$t('include')}}({{aggregate.includes.length}})</div>
            <div class="content">{{aggregate.includes.join(', ')}}</div>
          </div>
          <!-- Mandatory聚合组 -->
          <div class="row ksd-mb-15">
            <div class="title font-medium ksd-mb-10">{{$t('mandatory')}}({{aggregate.mandatory.length}})</div>
            <div class="content">{{aggregate.mandatory.join(', ')}}</div>
          </div>
          <!-- Hierarchy聚合组 -->
          <div class="row ksd-mb-15">
            <div class="title font-medium ksd-mb-10">{{$t('hierarchy')}}({{aggregate.hierarchyArray.length}})</div>
            <div class="list content"
              v-for="(hierarchy, hierarchyRowIdx) in aggregate.hierarchyArray"
              :key="`hierarchy-${hierarchyRowIdx}`">
              {{$t('group') + `-${hierarchyRowIdx + 1}`}}: <span>{{hierarchy.items.join(', ')}}</span>
            </div>
          </div>
          <!-- Joint聚合组 -->
          <div class="row ksd-mb-15">
            <div class="title font-medium ksd-mb-10">{{$t('joint')}}({{aggregate.jointArray.length}})</div>
            <div class="list content"
              v-for="(joint, jointRowIdx) in aggregate.jointArray"
              :key="`joint-${jointRowIdx}`">
              {{$t('group') + `-${jointRowIdx + 1}`}}: <span>{{joint.items.join(', ')}}</span>
            </div>
          </div>
        </template>
        <template v-else>
          <div class="row ksd-mb-15">
            <div class="title font-medium ksd-mb-10">{{$t('includeMeasure')}}</div>
            <div class="content">{{aggregate.measures.join(', ')}}</div>
          </div>
        </template>
      </div>
    </div>
    <div class="agg-detail-block" v-else>
      <div class="empty-block">
        <div>{{$t('aggGroupTips')}}</div>
        <el-button type="primary" text icon="el-icon-ksd-table_add" @click="handleAggregateGroup" v-if="isShowEditAgg">{{$t('aggGroup')}}</el-button>
      </div>
    </div>
    <AggAdvancedModal/>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, objectClone, handleError, getFullMapping, kapConfirm } from 'util'
import locales from './locales'
import AggAdvancedModal from './AggAdvancedModal/index.vue'
@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    },
    isShowEditAgg: {
      type: Boolean,
      default: true
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchAggregateGroups: 'FETCH_AGGREGATE_GROUPS',
      getCalcCuboids: 'GET_AGG_CUBOIDS',
      updateAggregateGroups: 'UPDATE_AGGREGATE_GROUPS'
    }),
    ...mapActions('AggAdvancedModal', {
      callAggAdvancedModal: 'CALL_MODAL'
    }),
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    })
  },
  components: {
    AggAdvancedModal
  },
  locales
})
export default class AggregateView extends Vue {
  aggType = 'custom'
  options = ['custom']
  aggregationObj = null
  aggregationGroups = []
  cuboidsInfo = {
    total_count: {},
    agg_index_counts: []
  }
  tabList = [
    {name: 'dimensions', key: 'dimension', size: 0, total: 0, target: 'includes'},
    {name: 'measures', key: 'measure', size: 0, total: 0, target: 'measures'}
  ]
  isLoading = false
  get dimensions () {
    if (this.model) {
      return this.model.simplified_dimensions
        .filter(column => column.status === 'DIMENSION')
        .map(dimension => ({
          label: dimension.column,
          value: dimension.column,
          id: dimension.id
        }))
    } else {
      return []
    }
  }
  get measures () {
    return this.model ? this.model.simplified_measures.map(measure => ({label: measure.name, value: measure.name, id: measure.id})) : []
  }
  totalSize (name) {
    return this[name].length
  }
  async handleAggregateGroup () {
    const { projectName, model } = this
    const isSubmit = await this.callAggregateModal({ editType: 'new', model, projectName })
    isSubmit && this.loadAggregateGroups()
    isSubmit && this.$emit('loadModels')
  }
  async editAggGroup (aggregateIdx) {
    const { projectName, model } = this
    const isSubmit = await this.callAggregateModal({ editType: 'edit', model, projectName, aggregateIdx: aggregateIdx + '' })
    isSubmit && this.loadAggregateGroups()
    isSubmit && this.$emit('loadModels')
  }
  async deleteAggGroup (aggregateIdx) {
    try {
      await kapConfirm(this.$t('delAggregateTip', {aggId: aggregateIdx + 1}), {type: 'warning', confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delAggregateTitle'))
      this.aggregationObj.aggregation_groups.splice(aggregateIdx, 1)
      const { projectName, model } = this
      await this.updateAggregateGroups({
        projectName,
        modelId: model.uuid,
        isCatchUp: false,
        globalDimCap: this.aggregationObj.global_dim_cap,
        aggregationGroups: this.aggregationObj.aggregation_groups
      })
      this.loadAggregateGroups()
    } catch (e) {
      handleError(e)
    }
  }
  // 打开高级设置
  openAggAdvancedModal () {
    this.callAggAdvancedModal({
      model: objectClone(this.model),
      aggIndexAdvancedDesc: null
    })
  }
  async mounted () {
    await this.loadAggregateGroups()
    this.$on('refresh', () => {
      this.loadAggregateGroups()
    })
  }
  async loadAggregateGroups () {
    this.isLoading = true
    try {
      const projectName = this.currentSelectedProject
      const modelId = this.model.uuid
      const nameMapping = this.getMapping(this.dimensions)
      const measuresMapping = this.getMapping(this.measures)
      const res = await this.fetchAggregateGroups({ projectName, modelId })
      const data = await handleSuccessAsync(res)
      if (data) {
        const calcRes = await this.getCalcCuboids({ projectName, modelId, aggregationGroups: data.aggregation_groups, globalDimCap: data.global_dim_cap })
        this.cuboidsInfo = await handleSuccessAsync(calcRes)

        this.aggregationObj = objectClone(data)
        this.aggregationGroups = data.aggregation_groups.map((aggregationGroup, aggregateIdx) => {
          aggregationGroup.id = data.aggregation_groups.length - aggregateIdx
          aggregationGroup.activeTab = 'dimension'
          aggregationGroup.includes = aggregationGroup.includes.map(include => nameMapping[include])
          aggregationGroup.measures = aggregationGroup.measures.map(measures => measuresMapping[measures])
          const selectRules = aggregationGroup.select_rule
          aggregationGroup.mandatory = selectRules.mandatory_dims.map(mandatory => nameMapping[mandatory])
          aggregationGroup.jointArray = selectRules.joint_dims.map((jointGroup, groupIdx) => {
            const items = jointGroup.map(joint => nameMapping[joint])
            return { id: groupIdx, items }
          })
          aggregationGroup.hierarchyArray = selectRules.hierarchy_dims.map((hierarchyGroup, groupIdx) => {
            const items = hierarchyGroup.map(hierarchy => nameMapping[hierarchy])
            return { id: groupIdx, items }
          })
          return aggregationGroup
        })
      }
      this.isLoading = false
    } catch (e) {
      handleError(e)
      this.isLoading = false
    }
  }
  getMapping (data) {
    const mapping = data.reduce((mapping, item) => {
      mapping[item.value] = item.id
      return mapping
    }, {})
    return getFullMapping(mapping)
  }
  scrollToMatched (index) {
    this.$nextTick(() => {
      const detailContents = this.$el.querySelectorAll('.agg-detail-block .agg-detail')
      this.$el.querySelector('.agg-detail-block').scrollTop = detailContents[index].offsetTop - 15
    })
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.model-aggregate-view {
  height: 500px;
  overflow: hidden;
  width: 100%;
  .title-list {
    width: 220px;
    height: 100%;
    box-shadow:2px 2px 4px 0px rgba(229,229,229,1);
    background-color: @fff;
    float:left;
    overflow-y: auto;
    padding-top: 15px;
    position: relative;
    .title-header {
      padding: 0 10px 10px 10px;
      line-height: 24px;
      .agg-types {
        width: 140px;
      }
    }
    .agg-title {
      height: 40px;
      line-height: 40px;
      cursor: pointer;
      padding-left: 10px;
      font-size: 14px;
      &:hover {
        background-color: @base-color-9;
      }
    }
  }
  .agg-total-info-block {
    background-color: @background-disabled-color;
    padding: 10px;
    color: @color-text-regular;
    font-size: 12px;
  }
  .agg-detail-block {
    height: 100%;
    margin: 15px 15px 15px 235px;
    overflow-y: auto;
    padding-bottom: 15px;
    position: relative;
    .content {
      color: @color-text-regular;
    }
    .icon-group i:hover {
      color: @base-color;
    }
    .agg-detail {
      padding: 15px;
      background-color: @fff;
      border: 1px solid @line-border-color4;
      .agg-content-title {
        height: 30px;
        line-height: 30px;
        margin-bottom: 10px;
        .agg-type {
          font-size: 12px;
          border: 1px solid @text-disabled-color;
          color: @text-disabled-color;
          border-radius: 2px;
          padding: 0 2px;
        }
        .divide {
          border-left: 1px solid @line-border-color;
          margin: 0 10px;
        }
        .dimCap-block {
          .nolimit-dim {
            color: @text-disabled-color;
          }
          .global-dim {
            font-style: oblique;
            color: @text-disabled-color;
          }
        }
      }
    }
    .empty-block {
      text-align: center;
      color:@text-disabled-color;
      position: absolute;
      top: 50%;
      text-align: center;
      width: 100%;
    }
  }
}
</style>
