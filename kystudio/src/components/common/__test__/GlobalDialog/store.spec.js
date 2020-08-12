import DetailDialogStore, { types } from '../../GlobalDialog/dialog/store'

const commitEvent = (name, params) => {
  DetailDialogStore.mutations[name](DetailDialogStore.state, params)
}

describe('DetailDialog store', () => {
  it('func', () => {
    DetailDialogStore.actions[types.CALL_MODAL]({commit: commitEvent}, {dialogType: 'error', submitText: '保存', showCopyBtn: true})
    expect(DetailDialogStore.state.isShow).toBeTruthy()

    DetailDialogStore.mutations[types.SET_MODAL](DetailDialogStore.state, {isShow: true, needResolveCancel: true})
    expect(DetailDialogStore.state.showDetailBtn).toBeTruthy()
    expect(DetailDialogStore.state.showDetailDirect).toBeFalsy()

    DetailDialogStore.mutations[types.RESET_MODAL](DetailDialogStore.state)
    expect(DetailDialogStore.state.submitText).toBe('保存')

    DetailDialogStore.mutations[types.HIDE_MODAL](DetailDialogStore.state)
    expect(DetailDialogStore.state.isShow).toBeFalsy()
  })
})
