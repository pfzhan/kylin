<template>
  <el-dialog class="demo-modal" limited-area :width="modalWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="isShow && closeHandler(false)">
    <!-- el-form表单 -->
    <div slot="footer" class="dialog-footer">
      <el-button plain size="medium" @click="closeHandler(false)">{{$t('cancel')}}</el-button>
      <el-button size="medium" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
    <!-- How to use? -->

    <!-- 1. 先依赖注入 -->
    <!-- eg:...mapActions('DemoModal', { -->
    <!--      callDemoModal: 'CALL_MODAL' -->
    <!--    }), -->

    <!-- 2. 局部组件引用在组件内进行引用 {components: {DemoModal}} and <DemoModal /> -->
    <!--    全局组件引用在/views/Modal中管理 -->

    <!-- 3. 在调用的函数中call Api -->
    <!--    const isSubmit = await this.callDemoModal({ editType: 'new' }) -->
    <!--    alert(isSubmit ? 'submit' : 'cancel') -->
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { fieldVisiableMaps, titleMaps, getSubmitData } from './handler'
import { validate, validateTypes, handleError } from '../../../util'

const { USERNAME } = validateTypes

vuex.registerModule(['modals', 'DemoModal'], store)

@Component({
  computed: {
    // Store数据注入
    ...mapState('DemoModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('DemoModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
    })
  },
  locales
})
export default class DemoModal extends Vue {
  // Data: 用来销毁el-form
  isFormShow = false
  // Data: el-form表单验证规则
  rules = {
    username: [{
      validator: this.validate(USERNAME), trigger: 'blur'
    }]
  }

  // Computed: Modal宽度
  get modalWidth () {
    return this.editType === 'new'
      ? '440px'
      : '660px'
  }

  // Computed: Modal标题
  get modalTitle () {
    return titleMaps[this.editType]
  }

  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }

  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }

  // Action: 模态框关闭函数
  closeHandler (isSubmit) {
    this.hideModal()

    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }

  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
  }

  // Action: Form递交函数
  async submit () {
    try {
      // 获取Form格式化后的递交数据
      /* const data = */getSubmitData(this)
      // 验证表单
      // await this.$refs['form'].validate()
      // 针对不同的模式，发送不同的请求
      // this.editType === 'new' && await this.saveUser(data)
      // 成功提示
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.saveSuccess')
      })
      // 关闭模态框，通知父组件成功
      this.closeHandler(true)
    } catch (e) {
      // 异常处理
      e && handleError(e)
    }
  }

  // Helper: 给el-form用的验证函数
  validate (type) {
    // TODO: 这里的this是vue的实例，而data却是class的实例
    return validate[type].bind(this)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
</style>
