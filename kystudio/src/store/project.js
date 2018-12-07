import api from './../service/api'
import * as types from './types'
import { cacheSessionStorage, cacheLocalStorage } from 'util/index'
import { speedProjectTypes } from 'config/index'
import { getAvailableOptions } from '../util/specParser'
export default {
  state: {
    projectList: [],
    allProject: [],
    projectTotalSize: 0,
    selected_project: localStorage.getItem('selected_project')
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list, size }) {
      state.projectList = list
      state.projectTotalSize = size
    },
    [types.SET_PROJECT]: function (state, project) {
      cacheSessionStorage('preProjectName', state.selected_project) // 储存上一次选中的project
      cacheSessionStorage('projectName', project)
      cacheLocalStorage('projectName', project)
      state.selected_project = project
    },
    [types.CACHE_ALL_PROJECTS]: function (state, { list, size }) {
      state.allProject.splice(0, state.allProject.length)
      state.allProject = Object.assign(state.allProject, list)
      var selectedProject = cacheSessionStorage('projectName') || cacheLocalStorage('projectName')
      var hasMatch = false
      if (list && list.length > 0) {
        list.forEach((p) => {
          if (p.name === selectedProject) {
            hasMatch = true
            state.selected_project = p.name // 之前没这句，在其他tab 切换了project，顶部会不变
          }
        })
        if (!hasMatch) {
          state.selected_project = state.allProject[0].name
          cacheSessionStorage('projectName', state.selected_project)
          cacheLocalStorage('projectName', state.selected_project)
        }
      } else {
        cacheSessionStorage('projectName', '')
        cacheLocalStorage('projectName', '')
        state.selected_project = ''
      }
    },
    [types.REMOVE_ALL_PROJECTS]: function (state) {
      state.allProject = []
    },
    [types.RESET_PROJECT_STATE]: function (state) {
      var selectedProject = cacheSessionStorage('projectName') || cacheLocalStorage('projectName')
      state.projectList.splice(0, state.projectList.length)
      state.allProject.splice(0, state.allProject.length)
      state.projectTotalSize = 0
      state.selected_project = selectedProject
      state.projectAccess = {}
      state.projectEndAccess = {}
    },
    [types.UPDATE_PROJECT] (state, { project }) {
      const projectIdx = state.allProject.findIndex(projectItem => projectItem.uuid === project.uuid)
      state.allProject = [
        ...state.allProject.slice(0, projectIdx),
        project,
        ...state.allProject.slice(projectIdx + 1)
      ]
    }
  },
  actions: {
    [types.LOAD_PROJECT_LIST]: function ({ commit }, params) {
      return api.project.getProjectList(params).then((response) => {
        commit(types.SAVE_PROJECT_LIST, {list: response.data.data.projects, size: response.data.data.size})
      })
    },
    [types.LOAD_ALL_PROJECT]: function ({ dispatch, commit, state }, params) {
      return new Promise((resolve, reject) => {
        api.project.getProjectList({pageOffset: 0, pageSize: 100000}).then((response) => {
          commit(types.CACHE_ALL_PROJECTS, {list: response.data.data.projects})
          let pl = response.data.data.projects && response.data.data.projects.length || 0
          if (!((params && params.ignoreAccess) || pl === 0)) {
            let curProjectUserAccessPromise = dispatch(types.USER_ACCESS, {project: state.selected_project})
            Promise.all([curProjectUserAccessPromise]).then(() => {
              resolve(response.data.data.projects)
            }, () => {
              resolve(response.data.data.projects)
            })
          } else {
            resolve(response.data.data.projects)
          }
        }, () => {
          commit(types.REMOVE_ALL_PROJECTS)
          reject()
        })
      })
    },
    [types.DELETE_PROJECT]: function ({ commit }, projectName) {
      return api.project.deleteProject(projectName)
    },
    [types.UPDATE_PROJECT]: function ({ commit }, project) {
      return api.project.updateProject(project)
        .then(response => {
          commit(types.UPDATE_PROJECT, { project: response.data.data })
        })
    },
    [types.SAVE_PROJECT]: function ({ dispatch, commit }, project) {
      return api.project.saveProject(project).then(async (res) => {
        await dispatch(types.LOAD_ALL_PROJECT)
        return res
      })
    },
    [types.SAVE_PROJECT_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.project.addProjectAccess(accessData, id)
    },
    [types.EDIT_PROJECT_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.project.editProjectAccess(accessData, id)
    },
    [types.GET_PROJECT_ACCESS]: function ({ commit }, para) {
      return api.project.getProjectAccess(para.projectId, para.data)
    },
    [types.DEL_PROJECT_ACCESS]: function ({ commit }, {id, aid, userName}) {
      return api.project.delProjectAccess(id, aid, userName)
    },
    [types.ADD_PROJECT_FILTER]: function ({ commit }, filterData) {
      return api.project.saveProjectFilter(filterData)
    },
    [types.EDIT_PROJECT_FILTER]: function ({ commit }, filterData) {
      return api.project.updateProjectFilter(filterData)
    },
    [types.DEL_PROJECT_FILTER]: function ({ commit }, {project, filterName}) {
      return api.project.delProjectFilter(project, filterName)
    },
    [types.GET_PROJECT_FILTER]: function ({ commit }, project) {
      return api.project.getProjectFilter(project)
    },
    [types.BACKUP_PROJECT]: function ({ commit }, project) {
      return api.project.backupProject(project)
    },
    [types.ACCESS_AVAILABLE_USER_OR_GROUP]: function ({ commit }, para) {
      return api.project.accessAvailableUserOrGroup(para.type, para.uuid, para.data)
    },
    [types.GET_QUOTA_INFO]: function ({ commit }, para) {
      return api.project.getQuotaInfo(para)
    },
    [types.CLEAR_TRASH]: function ({ commit }, para) {
      return api.project.clearTrash(para)
    }
  },
  getters: {
    projectActions: (state, getters, rootState, rootGetters) => {
      const groupRole = rootGetters.userAuthorities
      const projectRole = rootState.user.currentUserAccess

      return getAvailableOptions('projectActions', { groupRole, projectRole })
    },
    currentSelectedProject: (state) => {
      return state.selected_project
    },
    currentProjectData: (state, getters, rootState) => {
      const _filterable = state.allProject.filter(p => {
        return p.name === state.selected_project
      })
      if (Array.isArray(_filterable) && _filterable.length > 0) {
        return _filterable[0]
      }
      return null
    },
    isAutoProject: (state, getters) => {
      if (getters.currentProjectData) {
        const { maintain_model_type: maintainModelType } = getters.currentProjectData || {}
        return speedProjectTypes.includes(maintainModelType)
      }
    },
    selectedProjectDatasource: (state, getters, rootState) => {
      let datasourceKey = null

      const _filterable = state.allProject.filter(p => {
        return p.name === state.selected_project
      })
      if (Array.isArray(_filterable) && _filterable.length > 0) {
        datasourceKey = _filterable[0].override_kylin_properties['kylin.source.default']
      }

      return datasourceKey
    },
    globalDefaultDatasource: (state, getters, rootState) => {
      return rootState.system.sourceDefault
    }
  }
}
