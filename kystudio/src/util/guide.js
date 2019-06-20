import TWEEN from '@tweenjs/tween.js'
import store from '../store'
import { drama } from '../config/guide/index.js'
import Scrollbar from 'smooth-scrollbar'
import { normalGuideSpeed, queryLimit } from '../config/guide/config'
class Guide {
  constructor (options, _) {
    this.vm = _
    this.systemStore = store.state.system.guideConfig
    this.mode = options.mode // 模式
    this.steps = null
    this.guiding = false
    this.stepsInfo = []
    this.guideType = options.guideType
    this._mount = _.guideMount
    this.promiseObj = null
    this.STs = []
    this.stepSpeed = normalGuideSpeed
    this.waitLimit = queryLimit // 等待查询的次数限制
    this.waitCount = 0 // 等待次数
    this.currentStep = null
    this.isPause = false
    this.events = [
      {id: 1, info: '鼠标移动到指定位置', action: 'mouse'},
      {id: 11, info: '鼠标移动到指定位置', action: 'drag'},
      {id: 2, info: '点击', action: 'click'},
      {id: 21, info: '点击执行无点击效果', action: 'fakeclick'},
      {id: 3, info: '打字机输入', action: 'input'},
      {id: 31, info: '直接输入', action: 'inputonce'},
      {id: 32, info: '直接输入(非响应)', action: 'inputwithnoresponse'},
      {id: 5, info: '检查dom不存在', action: 'checkAbsent'},
      {id: 51, info: '检查dom存在', action: 'check'},
      {id: 6, info: '元素进入可视区域', action: 'inView'},
      {id: 8, info: '跳转路由', action: 'go'}
    ]
    this.drama = {
      manual: drama.manual(),
      autoProject: drama.autoProject(),
      project: drama.project(),
      loadTable: drama.loadTable(),
      autoLoadTable: drama.autoLoadTable(),
      addModel: drama.addModel(),
      monitor: drama.monitor(),
      query: drama.query(),
      acceleration: drama.acceleration()
    }
    this.stepsInfo = this.drama[this.mode]
    this.steps = this.stepFuncs(this.stepsInfo)
    this.vm.$set(this._mount, 'stepsInfo', this.stepsInfo)
  }
  getEventById (id) {
    for (let i = 0; i < this.events.length; i++) {
      if (this.events[i].id === id) {
        return this.events[i]
      }
    }
  }
  _click (el, stepInfo, resolve) {
    this.hideAllMouse()
    this.systemStore.globalMouseClick = true
    let st = setTimeout(() => {
      this.systemStore.globalMouseClick = false
      this.systemStore.globalMouseVisible = true
      if (stepInfo.withEvent) {
        el._isVue ? el.$el.click(stepInfo.val) : el.click(stepInfo.val)
      } else {
        el._isVue ? el.$emit('click', stepInfo.val) : el.click(stepInfo.val)
      }
      resolve()
    }, 100)
    this.STs.push(st)
  }
  _fakeclick (el, stepInfo, resolve) {
    el._isVue ? el.$emit('click', stepInfo.val) : el.click(stepInfo.val)
    resolve()
  }
  _focus (el) {
    el.focus && el.focus()
  }
  _input (el, val, resolve) {
    this._focus(el)
    this._printInput(el, val, resolve)
  }
  _inputonce (el, val, resolve) {
    this._focus(el)
    el && el.$emit('input', val)
  }
  _inputwithnoresponse (el, val, resolve) {
    this._focus(el)
    if (el) {
      el.$emit('change', val)
    }
  }
  _query (target, checkAbsent, search) {
    let result = null
    let targetResult = null
    let st = null
    let searchFuc = (resolve, reject) => {
      // 已经退出guide
      if (!this.systemStore.globalMaskVisible) {
        return reject()
      }
      if (!target && !search) {
        return resolve()
      }
      if (target) {
        this.waitCount++
        targetResult = this.systemStore.targetList[target]
        if (targetResult) {
          if (search) {
            let targetDom = targetResult.$el || targetResult
            result = targetDom.querySelector(search)
          } else {
            result = targetResult
          }
        } else {
          result = null
        }
      } else {
        result = document.querySelector(search)
      }
      if (checkAbsent && result || !checkAbsent && !result) {
        if (this.waitCount > this.waitLimit) {
          console.log('search timeout')
          return reject('timeout')
        }
        clearTimeout(st)
        st = setTimeout(() => {
          searchFuc(resolve, reject)
        }, 300)
        this.STs.push(st)
      } else {
        return resolve({dom: result, target: targetResult})
      }
    }
    return new Promise((resolve, reject) => {
      try {
        searchFuc(resolve, reject)
      } catch (e) {
        console.log(e)
        reject(e)
      }
    })
  }
  // 无拦截跳转路由
  _jump (name) {
    this.vm.$router.replace({name: 'refresh', params: { ignoreIntercept: true }})
    this.vm.$nextTick(() => {
      this.vm.$router.replace({name: name, params: { ignoreIntercept: true }})
    })
  }
  hideAllMouse () {
    this.systemStore.globalMouseVisible = false
    this.systemStore.globalMouseClick = false
    this.systemStore.globalMouseDrag = false
  }
  toggleMask (isShow) {
    this.systemStore.globalMaskVisible = isShow
  }
  // 手型移动
  _mouseTo (el, stepInfo, resolve) {
    if (el) {
      el = el.$el ? el.$el : el
    }
    this.hideAllMouse()
    if (stepInfo.isDrag) {
      this.systemStore.globalMouseDrag = true
    } else {
      this.systemStore.globalMouseVisible = true
    }
    let l = 0
    let t = 0
    if (stepInfo.pos) {
      l = stepInfo.pos.left
      t = stepInfo.pos.top
    } else {
      let calSize = el && el.getBoundingClientRect() || {}
      l = calSize.right
      t = calSize.bottom
    }
    let offsetX = stepInfo.offsetX || 0
    let offsetY = stepInfo.offsetY || 0
    new TWEEN.Tween(this.systemStore.mousePos).to({
      x: l - 40 + offsetX,
      y: t - 40 + offsetY
    }, 200).onComplete(() => {
      resolve()
    }).start()
    function animate () {
      if (TWEEN.update()) {
        requestAnimationFrame(animate)
      }
    }
    animate()
  }
  // 打字机输入效果
  _printInput (dom, val, resolve) {
    let i = 0
    dom.$emit('input', '')
    let fuc = () => {
      let st = setTimeout(() => {
        if (i > val.length) {
          resolve()
          return
        }
        let printVal = val.slice(0, i++)
        if (dom) {
          dom.$emit('input', printVal)
        }
        fuc()
      }, 50)
      this.STs.push(st)
    }
    fuc()
  }
  _inView (dom, targetDom, stepInfo, resolve) {
    if (targetDom) {
      targetDom = targetDom.$el ? targetDom.$el : targetDom
    }
    let scrollInstance = Scrollbar.get(targetDom)
    scrollInstance && scrollInstance.scrollIntoView(dom)
    let st = setTimeout(() => {
      if (scrollInstance && dom) {
        let listener = (status) => {
          if (scrollInstance.isVisible(dom)) {
            resolve()
            scrollInstance.removeListener(listener)
          }
        }
        scrollInstance.addListener(listener)
        scrollInstance.scrollIntoView(dom, {
          onlyScrollIfNeeded: true
        })
        listener() // 无滚动情况下直接执行
      } else {
        resolve()
      }
    }, 10)
    this.STs.push(st)
  }
  renderFuc (_event, stepInfo) {
    return new Promise((resolve, reject) => {
      this.waitCount = 0
      this._query(stepInfo.target, _event.action === 'checkAbsent', stepInfo.search).then((domList) => {
        let dom = domList && domList.dom || null
        let targetDom = domList && domList.target || null
        if (_event.action === 'mouse') {
          this._mouseTo(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (_event.action === 'drag') {
          stepInfo.isDrag = true
          this._mouseTo(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'click') {
          this._click(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'fakeclick') {
          this._fakeclick(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'input') {
          this._input(dom, stepInfo.val, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'inputonce') {
          this._inputonce(dom, stepInfo.val)
          resolve(stepInfo)
        } else if (dom && _event.action === 'inputwithnoresponse') {
          this._inputwithnoresponse(dom, stepInfo.val)
          resolve(stepInfo)
        } else if (_event.action === 'go') {
          this._jump(stepInfo.router)
          resolve(stepInfo)
        } else if (_event.action === 'inView') {
          this._inView(dom, targetDom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else {
          resolve(stepInfo)
        }
      }).catch((e) => {
        console.log(e)
        reject('timeout')
        console.log('guide timeout')
      })
    })
  }
  // 生成计划
  stepFuncs (funcs) {
    let result = []
    for (let i = 0; i < funcs.length; i++) {
      let stepEvent = this.getEventById(funcs[i].eventID)
      result.push(() => { return this.renderFuc(stepEvent, funcs[i]) })
    }
    return {
      steps: result,
      i: -1,
      next () {
        this.i++
        let step = this.steps[this.i]
        return {
          value: step && step() || null,
          done: this.i === this.steps.length - 1
        }
      },
      prev () {
        this.i--
        return {
          value: this.steps[this.i],
          done: false
        }
      }
    }
  }
  // 单步播放模式
  step (step, resolve, reject) {
    if (this.currentStep && this.currentStep.done === true || !this.currentStep) {
      let p = this.steps.next().value
      if (p && p.then) {
        p.then((stepInfo) => {
          this.currentStep = stepInfo
          this.vm.$set(stepInfo, 'done', true)
          if ((step === undefined || isNaN(step) || step - 1 > 0)) {
            let nextStep = () => {
              if (!this.isPause) {
                this.step(step - 1, resolve, reject)
              } else {
                let st = setTimeout(() => {
                  nextStep()
                }, 400)
                this.STs.push(st)
              }
            }
            let st = setTimeout(() => {
              nextStep()
            }, stepInfo.timer || this.stepSpeed)
            this.STs.push(st)
          }
        }, (res) => {
          // 超时抛出
          reject(res)
        })
      } else {
        this.guiding = false
        resolve('done')
        console.log('guide done')
      }
    } else {
      reject('guiding')
      console.log('guiding')
    }
  }
  getStepInfo (i) {
    return this.steps[i]
  }
  // 按步执行效果
  start () {
    this.toggleMask(true)
    return this
  }
  // 按步执行，step为空的话就连续执行
  go (step) {
    this.guiding = true
    this.isPause = false
    return new Promise((resolve, reject) => {
      this.step(step, resolve, reject)
    })
  }
  pause () {
    this.isPause = true
  }
  restart () {
    this.isPause = false
  }
  stop () {
    this.STs.forEach((i) => {
      clearTimeout(i)
    })
    this.toggleMask(false)
    this.hideAllMouse()
  }
}
export default Guide
