import store from '../store/index'

window.addEventListener('message', (args) => {
  const {data: {action, params}} = args
  if (action) {
    switch (action) {
      case 'filterModel':
        store.commit('UPDATE_FILTER_MODEL_NAME_CLOUD', params.modelAlias)
        break
    }
  }
})
