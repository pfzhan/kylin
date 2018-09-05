 <template>
    <div class="model_tool">
      <ul>
        <li class="toolbtn tool_add" @click="addZoom" v-unselect :title="$t('kylinLang.common.zoomIn')">
          <i class="el-icon-ksd-enlarge"></i>
        </li>
        <li class="toolbtn tool_jian" @click="subZoom" v-unselect :title="$t('kylinLang.common.zoomOut')">
          <i class="el-icon-ksd-shrink"></i>
        </li>
        <li class="zoom-size">{{Math.ceil(currentZoom*100)}}%</li>
        <li class="toolbtn" @click="autoLayerPosition" v-unselect  :title="$t('kylinLang.common.automaticlayout')">
          <i class="el-icon-ksd-auto"></i>
        </li>
        <li class="toolbtn" @click="toggleFullScreen" v-unselect  :title="$t('kylinLang.common.fullscreen')" v-if="kapPlatform!=='cloud'">
          <i :class="{'el-icon-ksd-full_02':isFull, 'el-icon-ksd-full_01': !isFull}"></i>
        </li>
      </ul>
      <slot name="morebtn"></slot>
    </div>
  </template>
  <script>
    export default {
      name: 'assist-tool',
      props: ['currentZoom'],
      data () {
        return {
          isFull: false
        }
      },
      computed: {
        kapPlatform () {
          return this.$store.state.config.platform
        }
      },
      methods: {
        addZoom () {
          this.$emit('addZoom')
        },
        subZoom () {
          this.$emit('subZoom')
        },
        autoLayerPosition () {
          this.$emit('autoLayerPosition')
        },
        toggleFullScreen () {
          this.$emit('toggleFullScreen')
          this.isFull = !this.isFull
        }
      }
    }
  </script>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
    .model_tool{
     position: absolute;
     right: 6px;
     z-index: 2000;
     top:80px;
     li{
      -webkit-select:none;
      user-select:none;
      -moz-user-select:none;
      // width: 24px;
      // height: 24px;
      // line-height: 24px;
      position: relative;
      // box-shadow: 1px 2px 1px rgba(0,0,0,.15);
      cursor: pointer;
      overflow: hidden;
      margin-top: 8px;
      width: 20px;
      height:20px;
      background-color: @fff;
      overflow: hidden;
       &.toolbtn{
          // width: 36px;
          // height: 36px;
          // background-color: #6d798a;
          color: @text-placeholder-color;
          // line-height: 36px;
          text-align: center;
          // margin-top: 10px;
          font-weight: bold;
          cursor: pointer;
       }
      i {
        font-size: 20px;
        color: @base-color; 
        &:hover{
          color: @base-color-2;
        }
      }
      &.zoom-size{
        box-shadow: none;
        background: none;
        color:@text-normal-color;
        // text-align: center;
        cursor:none;
        font-size:12px;
        width:32px;
      }
      span{
        display: block;
        width: 10px;
        height: 10px;
        position: absolute;
        top: 8px;
        left: 8px;
        background-size: 40px 10px;
      }
     }
     .tool_add{
      span{
        // background-image: url('../../../assets/img/outin.png');
        background-position: 0 0;
      }

     }
     .tool_jian{
       span {
        // background-image: url('../../../assets/img/outin.png');
        background-position: -10px 0
       }

     }
   }
  </style>
