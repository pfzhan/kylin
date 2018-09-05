 <template>
     <div class="tool_box">
           <common-tip :content="$t('openMutil')" placement="bottom-start" :class="{active:table.openMutilSelected}" v-if="mode!=='view'">
              <span @click="toggleMutilSelect"  >
                <i class="el-icon-ksd-dm" ></i>
              </span>
            </common-tip>
            <common-tip :content="$t('staticsTips')" placement="bottom-start">
              <span  @click="openModelSubMenu">
                <i class="el-icon-ksd-sample"></i>
              </span>
            </common-tip>
            <common-tip :content="$t('computedTips')" placement="bottom-start" v-show="table.kind!=='LOOKUP'">
              <span  @click="addComputedColumn">
                <i class="el-icon-ksd-computed" style="transform: scale(1.4);"></i>
              </span>
            </common-tip>
            <common-tip :content="$t('sortTips')" placement="bottom-start">
              <span @click="sortColumns">
                <i class="el-icon-ksd-az" style="transform: scale(1.4);"></i>
              </span>
            </common-tip>
            <common-tip :content="$t('inputSqlTips')" placement="bottom-start" v-show="table.kind == 'ROOTFACT'">
             <span  @click="inputSql">
              <i class="el-icon-ksd-sql"></i>
              </span>
            </common-tip>
            <!-- <i class="fa fa-window-close"></i> -->
            <common-tip :content="$t('selectTableTypeTips')" placement="right-start" class="ksd-fright" v-if="mode!=='view'">
              <el-dropdown @command="selectTableKind"  trigger="click">
                <span class="el-dropdown-link" style="display: block;height: 100%;width: 100%;">
                 <i class="el-icon-setting"></i>
                </span>
                <el-dropdown-menu slot="dropdown" >
                  <el-dropdown-item command="ROOTFACT" :data="table.guid">{{$t('kylinLang.common.fact')}}</el-dropdown-item>
                  <el-dropdown-item command="FACT" v-show="useLimitFact" :data="table.guid">{{$t('kylinLang.common.limitfact')}}</el-dropdown-item>
                  <el-dropdown-item command="LOOKUP" :data="table.guid">{{$t('kylinLang.common.lookup')}}</el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
            </common-tip>
        </div>
  </template>
  <script>
    import { mapGetters } from 'vuex'
    export default {
      name: 'table_tool',
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
        toggleMutilSelect () {
          this.$emit('toggleMutilSelect', this.table)
        },
        openModelSubMenu () {
          this.$emit('openModelSubMenu', 'hide', this.table.database, this.table.name)
        },
        addComputedColumn () {
          this.$emit('addComputedColumn', this.table.guid, this.table.kind)
        },
        sortColumns () {
          this.$emit('sortColumns', this.table)
        },
        inputSql () {
          this.$emit('inputSql')
        },
        selectTableKind (command, licompon) {
          this.$emit('selectTableKind', command, licompon)
        }
      },
      locales: {
        'en': {
          openMutil: 'Click here to set dimensions or measures, click again to exit.', staticsTips: 'Click here to view sampling data', computedTips: 'Click here to edit computed column', sortTips: 'Click here to ascend/descend columns', inputSqlTips: 'Click here to start model advisor: help you generate a complete model based on SQLs', selectTableTypeTips: 'Click here to set fact/lookup table'
        },
        'zh-cn': {
          openMutil: '点此设置维度和指标，再次点击可退出设置', staticsTips: '点此查看表的采样数据', computedTips: '点此设置可计算列', sortTips: '点此对列进行排序', inputSqlTips: '模型建议功能，可以根据输入的SQL补全模型', selectTableTypeTips: '点此设置事实表／维度表'
        }
      }

    }
  </script>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .tool_box{
      position: absolute;
      // padding:8px 6px;
      top:36px;
      height: 30px;
      line-height: 30px;
      left: 0;
      right: 0;
      font-size: 12px;
      color:@text-title-color;
      cursor: pointer;
      background-color: @line-border-color;
      span{
        &:hover, &.active{
          background-color: @base-color;
          color:@fff;
        }
      }
      .el-dropdown{
        width: 28px;
        text-align: center;
        color:@text-title-color;
        &:hover{
          .el-icon-setting {
            background-color: @base-color;
            color:@fff;
          }
        }
      }
      >span{
        float: left;
        // margin-right: 4px;
        display: block;
        height: 30px;
        margin-top: 0px;
        padding-top: 0px;
        // line-height: 30px;
        width: 28px;
        text-align:center;
      }
      i{
        cursor: pointer;
        transform: scale(1.4);
      }
      .fa_icon{
        cursor: pointer;
        vertical-align: center;
      }
    }
  </style>
