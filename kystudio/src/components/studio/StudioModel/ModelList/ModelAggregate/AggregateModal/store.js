import { handleSuccessAsync, getFullMapping } from '../../../../../../util'

const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM',
  SHOW_LOADING: 'SHOW_LOADING',
  HIDE_LOADING: 'HIDE_LOADING'
}
export const initialAggregateData = JSON.stringify({
  id: 0,
  includes: [],
  mandatory: [],
  jointArray: [{
    id: 0,
    items: []
  }],
  hierarchyArray: [{
    id: 0,
    items: []
  }]
})
const initialState = JSON.stringify({
  isShow: false,
  isLoading: false,
  editType: 'edit',
  callback: null,
  model: null,
  projectName: null,
  form: {
    isCatchUp: true,
    aggregateArray: [
      JSON.parse(initialAggregateData)
    ]
  }
})

export default {
  state: JSON.parse(initialState),
  getters: {
    dimensions (state) {
      return getDimensions(state.model)
    },
    dimensionIdMapping (state, getters) {
      return getMapping(getters.dimensions)
    }
  },
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
    },
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SHOW_LOADING]: (state) => {
      state.isLoading = true
    },
    [types.HIDE_LOADING]: (state) => {
      state.isLoading = false
    },
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    },
    [types.SET_MODAL]: (state, payload) => {
      for (const key of Object.keys(state)) {
        payload[key] && (state[key] = payload[key])
      }
    },
    [types.INIT_FORM]: (state, payload) => {
      if (payload) {
        const dimensions = getDimensions(state.model)
        const nameMapping = getMapping(dimensions)
        state.form.aggregateArray = payload.aggregation_groups.map((aggregationGroup, aggregateIdx) => {
          const id = payload.aggregation_groups.length - aggregateIdx
          const includes = aggregationGroup.includes.map(include => nameMapping[include])
          const selectRules = aggregationGroup.select_rule
          const mandatory = selectRules.mandatory_dims.map(mandatory => nameMapping[mandatory])
          const jointArray = selectRules.joint_dims.map((jointGroup, groupIdx) => {
            const items = jointGroup.map(joint => nameMapping[joint])
            return { id: groupIdx, items }
          })
          const hierarchyArray = selectRules.hierarchy_dims.map((hierarchyGroup, groupIdx) => {
            const items = hierarchyGroup.map(hierarchy => nameMapping[hierarchy])
            return { id: groupIdx, items }
          })
          if (!hierarchyArray.length) {
            hierarchyArray.push({ id: 0, items: [] })
          }
          if (!jointArray.length) {
            jointArray.push({ id: 0, items: [] })
          }
          return { id, includes, mandatory, jointArray, hierarchyArray }
        })
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, projectName, model }) {
      const { dispatch } = this

      return new Promise(async (resolve, reject) => {
        const modelId = model && model.uuid

        commit(types.SET_MODAL, { editType, model, projectName, callback: resolve })
        commit(types.SHOW_LOADING)
        commit(types.SHOW_MODAL)
        const response = await dispatch('FETCH_AGGREGATE_GROUPS', { projectName, modelId })
        const aggregateGroupRule = await handleSuccessAsync(response)
        setTimeout(() => {
          commit(types.INIT_FORM, aggregateGroupRule)
        }, 0)
        commit(types.HIDE_LOADING)
      })
    }
  },
  namespaced: true
}

function getMapping (dimensions) {
  const mapping = dimensions.reduce((mapping, item) => {
    mapping[item.value] = item.id
    return mapping
  }, {})
  return getFullMapping(mapping)
}

function getDimensions (model) {
  if (model) {
    return model.simplified_dimensions
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

export { types }
