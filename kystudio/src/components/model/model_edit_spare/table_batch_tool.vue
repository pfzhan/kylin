 <template>
     <div v-show="table.openMutilSelected" class="sub_tool">
        <ul>
          <li style="vertical-align:top">
            <common-tip :tips="$t('checkAllColumns')" placement="top-start"><el-checkbox  v-model="table.allChecked" @change="checkAllColumns(table)"></el-checkbox></common-tip>
          </li>

          <li class="dm-btn" :class="{'active': table.mutilSelectedList && table.mutilSelectedList.length > 0}">
            <common-tip :tips="$t('unSelectColumnsTip')" trigger="manual" :disabled="!!(table.mutilSelectedList && table.mutilSelectedList.length)"  placement="top-start">
              <span @click="changeSelectedColumnKind(table, 'D')" style="cursor: pointer;color:#7881AA">
                <i class="el-icon-ksd-model_d"></i>
              </span>
              <span @click="changeSelectedColumnKind(table, 'M')" style="cursor: pointer;color:#7881AA">
                <i class="el-icon-ksd-model_m"></i>
              </span>
              <span @click="changeSelectedColumnKind(table, '-')" style="cursor: pointer;color:#7881AA">
                <i class="el-icon-ksd-model_dis"></i>
              </span>
            </common-tip>
          </li>
          <li class="dm-btn" :class="{'active': table.mutilSelectedList && table.mutilSelectedList.length > 0}">
            <common-tip :tips="$t('autoSuggestTip')">
              <span @click="autoSuggestDM(table)" style="cursor: pointer;color:#7881AA">
                <i class="el-icon-ksd-model_a"></i>
              </span>
            </common-tip>
          </li>
        </ul>
      </div>
  </template>
  <script>
    import { mapGetters } from 'vuex'
    export default {
      name: 'table_batch_tool',
      props: ['table', 'mode'],
      computed: {
        ...mapGetters([
          'selectedProjectDatasource'
        ]),
        useLimitFact () {
          return this.$store.state.system.limitlookup === 'false'
        }
      },
      methods: {
        checkAllColumns () {
          this.$emit('checkAllColumns', this.table)
        },
        changeSelectedColumnKind (table, kind) {
          this.$emit('changeSelectedColumnKind', table, kind)
        },
        autoSuggestDM (table) {
          this.$emit('autoSuggestDM', table)
        }
      },
      locales: {
        'en': {
          checkAllColumns: 'Choose all columns.', unSelectColumnsTip: 'Click column name to choose it.', autoSuggestTip: 'Suggesting dimensions and measures'
        },
        'zh-cn': {
          checkAllColumns: '选择所有列', unSelectColumnsTip: '请先点击下面的列名选中您需要修改的列', autoSuggestTip: '系统推荐维度和指标'
        }
      }

    }
  </script>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .sub_tool {
    cursor: auto;
    border-bottom: solid 1px @line-border-color;
    width:220px;
    height:26px;
    position: relative;
    ul{
      li{
        &.dm-btn{
          // vertical-align: unset;
        }
        display: inline-block;
        min-width: 26px;
        line-height: 28px;
        height: 28px;
        text-align: center;
        i {
          font-size: 14px;
          margin-left: 4px;
          &:hover{
            color:@base-color;
          }
        }
      }
    }
   }
  </style>
