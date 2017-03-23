<template>
<div>
  <el-steps :active="activeStep"  finish-status="finish" process-status="wait" center >
    <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
    <el-step :title="$t('dimensions')" @click.native="step(2)"></el-step>
    <el-step :title="$t('measures')" @click.native="step(3)"></el-step>
    <el-step :title="$t('refreshSetting')" @click.native="step(4)"></el-step>
    <el-step :title="$t('advancedSetting')" @click.native="step(5)"></el-step>
    <el-step :title="$t('configurationOverwrites')" @click.native="step(6)"></el-step>
    <el-step :title="$t('overview')" @click.native="step(7)"></el-step>
  </el-steps>
  <info v-if="activeStep===1" :cubeDesc="selected_cube" :isEdit="isEdit"></info>
  <dimensions v-if="activeStep===2" :cubeDesc="selected_cube" :modelDesc="model" :isEdit="isEdit"></dimensions>
  <measures v-if="activeStep===3" :cubeDesc="selected_cube" :isEdit="isEdit"></measures>
  <refresh_setting v-if="activeStep===4" :cubeDesc="selected_cube" :isEdit="isEdit"></refresh_setting>
  <advanced_setting v-if="activeStep===5" :cubeDesc="selected_cube" :isEdit="isEdit"></advanced_setting>
  <configuration_overwrites v-if="activeStep===6" :cubeDesc="selected_cube" :isEdit="isEdit"></configuration_overwrites>
  <overview v-if="activeStep===7" :cubeDesc="selected_cube" :isEdit="isEdit"></overview>
  <el-button icon="arrow-left" v-if="activeStep !== 1" @click.native="prev">{{$t('prev')}}</el-button>
  <el-button type="success" v-if="activeStep !== 7" @click.native="next">{{$t('next')}}<i class="el-icon-arrow-right el-icon--right"></i></el-button>
  <el-button type="primary" v-if="activeStep === 7" @click.native="save">{{$t('save')}}</el-button>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import info from './info_edit'
import dimensions from './dimensions_edit'
import measures from './measures_edit'
import refreshSetting from './refresh_setting_edit'
import advancedSetting from './advanced_setting_edit'
import configurationOverwrites from './configuration_overwrites_edit'
import overview from './overview_edit'
export default {
  name: 'cubeDescEdit',
  data () {
    return {
      activeStep: 1,
      isEdit: true,
      model: {
        'uuid': '72ab4ee2-2cdb-4b07-b39e-4c298563ae27',
        'last_modified': 1485169522000,
        'version': '2.0.0',
        'name': 'ci_inner_join_model',
        'owner': null,
        'description': null,
        'fact_table': 'DEFAULT.TEST_KYLIN_FACT',
        'lookups': [
          {
            'table': 'DEFAULT.TEST_ORDER',
            'kind': 'FACT',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'TEST_ORDER.ORDER_ID'
              ],
              'foreign_key': [
                'TEST_KYLIN_FACT.ORDER_ID'
              ]
            }
          },
          {
            'table': 'DEFAULT.TEST_ACCOUNT',
            'kind': 'FACT',
            'alias': 'SELLER_ACCOUNT',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'SELLER_ACCOUNT.ACCOUNT_ID'
              ],
              'foreign_key': [
                'TEST_KYLIN_FACT.SELLER_ID'
              ]
            }
          },
          {
            'table': 'EDW.TEST_CAL_DT',
            'kind': 'LOOKUP',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'TEST_CAL_DT.CAL_DT'
              ],
              'foreign_key': [
                'TEST_KYLIN_FACT.CAL_DT'
              ]
            }
          },
          {
            'table': 'DEFAULT.TEST_CATEGORY_GROUPINGS',
            'kind': 'LOOKUP',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID',
                'TEST_CATEGORY_GROUPINGS.SITE_ID'
              ],
              'foreign_key': [
                'TEST_KYLIN_FACT.LEAF_CATEG_ID',
                'TEST_KYLIN_FACT.LSTG_SITE_ID'
              ]
            }
          },
          {
            'table': 'EDW.TEST_SITES',
            'kind': 'LOOKUP',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'TEST_SITES.SITE_ID'
              ],
              'foreign_key': [
                'TEST_KYLIN_FACT.LSTG_SITE_ID'
              ]
            }
          },
          {
            'table': 'EDW.TEST_SELLER_TYPE_DIM',
            'kind': 'LOOKUP',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD'
              ],
              'foreign_key': [
                'TEST_KYLIN_FACT.SLR_SEGMENT_CD'
              ]
            }
          },
          {
            'table': 'DEFAULT.TEST_ACCOUNT',
            'kind': 'FACT',
            'alias': 'BUYER_ACCOUNT',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'BUYER_ACCOUNT.ACCOUNT_ID'
              ],
              'foreign_key': [
                'TEST_ORDER.BUYER_ID'
              ]
            }
          },
          {
            'table': 'DEFAULT.TEST_COUNTRY',
            'kind': 'LOOKUP',
            'alias': 'SELLER_COUNTRY',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'SELLER_COUNTRY.COUNTRY'
              ],
              'foreign_key': [
                'SELLER_ACCOUNT.ACCOUNT_COUNTRY'
              ]
            }
          },
          {
            'table': 'DEFAULT.TEST_COUNTRY',
            'kind': 'LOOKUP',
            'alias': 'BUYER_COUNTRY',
            'join': {
              'type': 'INNER',
              'primary_key': [
                'BUYER_COUNTRY.COUNTRY'
              ],
              'foreign_key': [
                'BUYER_ACCOUNT.ACCOUNT_COUNTRY'
              ]
            }
          }
        ],
        'dimensions': [
          {
            'table': 'TEST_KYLIN_FACT',
            'columns': [
              'TRANS_ID',
              'ORDER_ID',
              'CAL_DT',
              'LSTG_FORMAT_NAME',
              'LSTG_SITE_ID',
              'LEAF_CATEG_ID',
              'SLR_SEGMENT_CD',
              'SELLER_ID',
              'TEST_COUNT_DISTINCT_BITMAP'
            ]
          },
          {
            'table': 'TEST_ORDER',
            'columns': [
              'ORDER_ID',
              'BUYER_ID',
              'TEST_DATE_ENC',
              'TEST_TIME_ENC',
              'TEST_EXTENDED_COLUMN'
            ]
          },
          {
            'table': 'BUYER_ACCOUNT',
            'columns': [
              'ACCOUNT_ID',
              'ACCOUNT_BUYER_LEVEL',
              'ACCOUNT_SELLER_LEVEL',
              'ACCOUNT_COUNTRY',
              'ACCOUNT_CONTACT'
            ]
          },
          {
            'table': 'SELLER_ACCOUNT',
            'columns': [
              'ACCOUNT_ID',
              'ACCOUNT_BUYER_LEVEL',
              'ACCOUNT_SELLER_LEVEL',
              'ACCOUNT_COUNTRY',
              'ACCOUNT_CONTACT'
            ]
          },
          {
            'table': 'TEST_CATEGORY_GROUPINGS',
            'columns': [
              'LEAF_CATEG_ID',
              'SITE_ID',
              'META_CATEG_NAME',
              'CATEG_LVL2_NAME',
              'CATEG_LVL3_NAME',
              'USER_DEFINED_FIELD1',
              'USER_DEFINED_FIELD3',
              'UPD_DATE',
              'UPD_USER'
            ]
          },
          {
            'table': 'TEST_SITES',
            'columns': [
              'SITE_ID',
              'SITE_NAME',
              'CRE_USER'
            ]
          },
          {
            'table': 'TEST_SELLER_TYPE_DIM',
            'columns': [
              'SELLER_TYPE_CD',
              'SELLER_TYPE_DESC'
            ]
          },
          {
            'table': 'TEST_CAL_DT',
            'columns': [
              'CAL_DT',
              'WEEK_BEG_DT'
            ]
          },
          {
            'table': 'BUYER_COUNTRY',
            'columns': [
              'COUNTRY',
              'NAME'
            ]
          },
          {
            'table': 'SELLER_COUNTRY',
            'columns': [
              'COUNTRY',
              'NAME'
            ]
          }
        ],
        'metrics': [
          'TEST_KYLIN_FACT.PRICE',
          'TEST_KYLIN_FACT.ITEM_COUNT'
        ],
        'filter_condition': null,
        'partition_desc': {
          'partition_date_column': 'TEST_KYLIN_FACT.CAL_DT',
          'partition_time_column': null,
          'partition_date_start': 0,
          'partition_date_format': 'yyyy-MM-dd',
          'partition_time_format': 'HH:mm:ss',
          'partition_type': 'APPEND',
          'partition_condition_builder': 'org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder'
        },
        'capacity': 'MEDIUM'
      },
      selected_project: this.$store.state.project.selected_project
    }
  },
  components: {
    'info': info,
    'dimensions': dimensions,
    'measures': measures,
    'refresh_setting': refreshSetting,
    'advanced_setting': advancedSetting,
    'configuration_overwrites': configurationOverwrites,
    'overview': overview
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC'
    }),
    step: function (num) {
      this.activeStep = num
    },
    prev: function () {
      this.activeStep = this.activeStep - 1
    },
    next: function () {
      this.activeStep = this.activeStep + 1
    },
    createNewCube: function () {
      this.isEdit = false
      this.$store.state.cube.cubeAdd = {
        'name': '',
        'model_name': '',
        'description': '',
        'dimensions': [],
        'measures': [
          {
            'name': '_COUNT_',
            'function': {
              'expression': 'COUNT',
              'returntype': 'bigint',
              'parameter': {
                'type': 'constant',
                'value': '1',
                'next_parameter': null
              },
              'configuration': {}
            }
          }
        ],
        'rowkey': {
          'rowkey_columns': []
        },
        'aggregation_groups': [],
        'dictionaries': [],
        'partition_date_start': 0,
        'partition_date_end': undefined,
        'notify_list': [],
        'hbase_mapping': {
          'column_family': []
        },
        'status_need_notify': ['ERROR', 'DISCARDED', 'SUCCEED'],
        'retention_range': '0',
        'auto_merge_time_ranges': [604800000, 2419200000],
        'engine_type': 0,
        'storage_type': 0,
        'override_kylin_properties': {}
      }
    }
  },
  created () {
    this.createNewCube()
  },
  computed: {
    selected_cube: function () {
      return this.$store.state.cube.cubeAdd
    }
  },
  locales: {
    'en': {cubeInfo: 'Cube Info', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', advancedSetting: 'Advanced Setting', configurationOverwrites: 'Configuration Overwrites', overview: 'Overview', prev: 'Prev', next: 'Next', save: 'Save'},
    'zh-cn': {cubeInfo: 'Cube信息', dimensions: '维度', measures: '度量', refreshSetting: '更新配置', advancedSetting: '高级设置', configurationOverwrites: '配置覆盖', overview: '概览', prev: 'Prev', next: 'Next', save: 'Save'}
  }
}
</script>
<style scoped="">

</style>
