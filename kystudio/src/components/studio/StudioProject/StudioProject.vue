<template>
  <div class="studio-project table-layout clearfix">
    <ProjectSidebar class="layout-left" :project="currentProjectData" />
    <div class="layout-right">
      <template v-if="currentProjectData">
        <!-- Project标题信息 -->
        <div class="project-header">
          <h1 class="project-name">{{projectInfo.name}}</h1>
          <h2 class="project-update-at">{{$t('updateAt')}} {{projectInfo.updateAt}}</h2>
        </div>
        <!-- Project详细信息 -->
        <div class="project-details">
          <el-tabs v-model="viewType">
            <el-tab-pane :label="$t('detail')" :name="viewTypes.DETAIL">
              <el-row :gutter="30">
                <el-col :span="5">
                  <el-card class="detail-list source-list">
                    <div class="header" slot="header">{{$t('source')}}</div>
                    <ul class="body" :style="{height: `${windowHeight - 320}px`}">
                      <template v-if="projectInfo.datasources.length">
                        <li class="datasouce" v-if="projectInfo.datasources.includes(sourceTypes.HIVE)">
                          <div class="datasource-icon">
                            <i class="el-icon-ksd-hive"></i>
                          </div>
                          <div class="datasource-name">Hive</div>
                        </li>
                      </template>
                      <template v-else>
                        <li class="empty">
                          <div>
                            <img class="empty-img" src="../../../assets/img/no_data.png" />
                          </div>
                          <div>{{$t('kylinLang.common.noData')}}</div>
                        </li>
                      </template>
                    </ul>
                  </el-card>
                </el-col>
                <el-col :span="9">
                  <el-card class="detail-list">
                    <div class="header" slot="header">{{$t('table')}}</div>
                    <ul class="body" :style="{height: `${windowHeight - 320}px`}">
                      <template v-if="projectInfo.tables.length">
                        <li class="datasouce" v-if="projectInfo.tables.includes(sourceTypes.HIVE)">
                          <div class="datasource-icon">
                            <i class="el-icon-ksd-hive"></i>
                          </div>
                          <div class="datasource-name">Hive</div>
                        </li>
                      </template>
                      <template v-else>
                        <li class="empty">
                          <div>
                            <img class="empty-img" src="../../../assets/img/no_data.png" />
                          </div>
                          <div>{{$t('kylinLang.common.noData')}}</div>
                        </li>
                      </template>
                    </ul>
                  </el-card>
                </el-col>
                <el-col :span="10">
                  <el-card class="detail-list">
                    <div class="header" slot="header">{{$t('model')}}</div>
                    <ul class="body" :style="{height: `${windowHeight - 320}px`}">
                      <template v-if="projectInfo.models.length">
                        <li class="datasouce" v-if="projectInfo.models.includes(sourceTypes.HIVE)">
                          <div class="datasource-icon">
                            <i class="el-icon-ksd-hive"></i>
                          </div>
                          <div class="datasource-name">Hive</div>
                        </li>
                      </template>
                      <template v-else>
                        <li class="empty">
                          <div>
                            <img class="empty-img" src="../../../assets/img/no_data.png" />
                          </div>
                          <div>{{$t('kylinLang.common.noData')}}</div>
                        </li>
                      </template>
                    </ul>
                  </el-card>
                </el-col>
              </el-row>
            </el-tab-pane>
            <el-tab-pane :label="$t('dataCheck')" :name="viewTypes.DATA_CHECK">
              <el-row style="margin-bottom: 10px;">
                <span class="switch-title">{{$t('isDataCheck')}}</span>
                <el-switch
                  :value="form.isDataCheck"
                  active-text="OFF"
                  inactive-text="ON">
                </el-switch>
              </el-row>
              <el-row>
                <el-table border :data="tableData" v-show="false">
                  <el-table-column
                    prop="title"
                    :label="$t('title')"
                    min-width="150px">
                  </el-table-column>
                  <el-table-column
                    prop="description"
                    :label="$t('description')"
                    min-width="320px">
                  </el-table-column>
                  <el-table-column
                    prop="value"
                    :label="$t('value')"
                    min-width="110px"
                    align="center">
                    <template slot-scope="scope">
                      <el-checkbox :value="form[scope.row.key]"></el-checkbox>
                    </template>
                  </el-table-column>
                </el-table>
              </el-row>
            </el-tab-pane>
            <el-tab-pane :label="$t('configuration')" :name="viewTypes.CONFIGURATION">
            </el-tab-pane>
            <!-- <el-tab-pane :label="$t('other')" :name="viewTypes.OTHER">
            </el-tab-pane> -->
          </el-tabs>
        </div>
      </template>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import dayjs from 'dayjs'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes } from './handler'
import { sourceTypes } from '../../../config'
import ProjectSidebar from '../../common/ProjectSidebar/ProjectSidebar.vue'

@Component({
  computed: {
    ...mapGetters([
      'currentProjectData'
    ])
  },
  components: {
    ProjectSidebar
  },
  locales
})
export default class StudioProject extends Vue {
  viewType = viewTypes.DETAIL
  viewTypes = viewTypes
  sourceTypes = sourceTypes
  windowHeight = 0
  tables = []
  models = []
  form = {
    isDataCheck: true,
    isDataSkew: true,
    isKeyDuplicate: true,
    isReservedWords: true,
    isSpecialCharacter: true
  }
  get tableData () {
    const ignoreTableRow = ['isDataCheck']

    return Object.entries(this.form)
      .filter(([key, value]) => !ignoreTableRow.includes(key))
      .map(([key, value]) => ({
        key,
        title: this.$t(key),
        description: this.$t(`${key}Desc`)
      }))
  }
  get projectInfo () {
    const datasource = this.currentProjectData &&
      this.currentProjectData.override_kylin_properties &&
      this.currentProjectData.override_kylin_properties['kylin.source.default']
    const datasources = datasource ? [+datasource] : []
    return {
      name: this.currentProjectData.name,
      updateAt: dayjs(this.currentProjectData.last_modified).format('YYYY-MM-DD HH:mm:ss G[MT]Z'),
      datasources,
      tables: [],
      models: []
    }
  }
  refreshHeight () {
    this.windowHeight = window.innerHeight
  }
  handleResize () {
    this.refreshHeight()
  }
  mounted () {
    this.handleResize()
    window.addEventListener('resize', this.handleResize)
  }
  beforeDestory () {
    window.removeEventListener('resize', this.handleResize)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.studio-project {
  height: 100%;
  background: white;
  .project-name {
    font-size: 16px;
    color: #263238;
    margin-bottom: 15px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .project-details {
    padding-bottom: 20px;
  }
  .project-update-at {
    font-size: 12px;
    color: #8E9FA8;
    font-weight: normal;
  }
  .project-header {
    padding-right: 300px;
    position: relative;
    margin-bottom: 12px;
  }
  .el-tabs__nav {
    margin-left: 0;
  }
  .detail-list {
    width: 100%;
    box-sizing: border-box;
    .header {
      text-align: center;
    }
    .body {
      position: relative;
      min-height: 334px;
      overflow: auto;
      box-sizing: border-box;
    }
  }
  .source-list {
    .body {
      padding: 50px 0;
    }
  }
  .datasource-icon {
    font-size: 65px;
    width: 90px;
    height: 90px;
    line-height: 90px;
    text-align: center;
    border-radius: 6px;
    background: @grey-4;
    margin: 0 auto 10px auto;
    color: @base-color;
    border: 1px solid @base-color;
    * {
      cursor: default;
    }
  }
  .datasource-name {
    text-align: center;
    color: @base-color;
    font-size: 16px;
  }
  .empty {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
  }
  .empty-img {
    width: 60px;
  }

  .el-table__header-wrapper {
    .cell {
      text-align: center;
    }
  }
  .switch-title {
    color: @text-title-color;
    vertical-align: middle;
  }
}
</style>
