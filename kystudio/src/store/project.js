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
    projectAutoApplyConfig: false,
    isAllProject: false,
    selected_project: cacheSessionStorage('projectName') || cacheLocalStorage('projectName'),
    projectDefaultDB: '',
    isSemiAutomatic: false
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list, size }) {
      state.projectList = list
      state.projectTotalSize = size
    },
    [types.UPDATE_PROJECT_SEMI_AUTOMATIC_STATUS]: function (state, result) {
      state.isSemiAutomatic = result
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
            state.isSemiAutomatic = p.override_kylin_properties && p.override_kylin_properties['kap.metadata.semi-automatic-mode'] === 'true' // 获取当前project 是否含有半自动的标志
            state.projectDefaultDB = p.default_database
          }
        })
        if (!hasMatch) {
          state.selected_project = state.allProject[0].name
          state.isSemiAutomatic = state.allProject[0].override_kylin_properties && state.allProject[0].override_kylin_properties['kap.metadata.semi-automatic-mode'] === 'true'
          cacheSessionStorage('projectName', state.selected_project)
          cacheLocalStorage('projectName', state.selected_project)
          state.projectDefaultDB = state.allProject[0].default_database
        }
      } else {
        cacheSessionStorage('projectName', '')
        cacheLocalStorage('projectName', '')
        state.selected_project = ''
        state.isSemiAutomatic = false // 如果接口没取到，默认设为false
        state.projectDefaultDB = ''
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
    },
    [types.CACHE_PROJECT_TIPS_CONFIG]: function (state, { projectAutoApplyConfig }) {
      state.projectAutoApplyConfig = projectAutoApplyConfig
    },
    [types.CACHE_PROJECT_DEFAULT_DB]: function (state, { projectDefaultDB }) {
      state.projectDefaultDB = projectDefaultDB
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
            let curProjectConfigPromise = dispatch(types.FETCH_PROJECT_SETTINGS, {projectName: state.selected_project})
            Promise.all([curProjectUserAccessPromise, curProjectConfigPromise]).then(() => {
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
        // await dispatch(types.LOAD_ALL_PROJECT)
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
    [types.DEL_PROJECT_ACCESS]: function ({ commit }, {id, aid, userName, principal}) {
      return api.project.delProjectAccess(id, aid, userName, principal)
    },
    [types.SUBMIT_ACCESS_DATA]: function ({ commit }, {projectName, userType, roleOrName, accessData}) {
      return api.project.submitAccessData(projectName, userType, roleOrName, accessData)
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
    },
    [types.FETCH_PROJECT_SETTINGS]: function ({ commit }, para) {
      return api.project.fetchProjectSettings(para.projectName).then((response) => {
        commit(types.CACHE_PROJECT_TIPS_CONFIG, {projectAutoApplyConfig: response.data.data.tips_enabled})
        commit(types.CACHE_PROJECT_DEFAULT_DB, {projectDefaultDB: response.data.data.default_database})
        // 更新是否是半自动档标志
        commit(types.UPDATE_PROJECT_SEMI_AUTOMATIC_STATUS, response.data.data.semi_automatic_mode)
        return response
      })
    },
    [types.UPDATE_PROJECT_GENERAL_INFO]: function ({ commit }, para) {
      return api.project.updateProjectGeneralInfo(para)
    },
    [types.UPDATE_SEGMENT_CONFIG]: function ({ commit }, para) {
      return api.project.updateSegmentConfig(para)
    },
    [types.UPDATE_PUSHDOWN_CONFIG]: function ({commit}, para) {
      return api.project.updatePushdownConfig(para)
    },
    [types.UPDATE_STORAGE_QUOTA]: function ({commit}, para) {
      return api.project.updateStorageQuota(para)
    },
    [types.UPDATE_ACCELERATION_SETTINGS]: function ({commit}, para) {
      return api.project.updateAccelerationSettings(para).then((response) => {
        commit(types.CACHE_PROJECT_TIPS_CONFIG, {projectAutoApplyConfig: para.tips_enabled})
        return response
      })
    },
    [types.UPDATE_JOB_ALERT_SETTINGS]: function ({commit}, para) {
      return api.project.updateJobAlertSettings(para)
    },
    [types.UPDATE_PROJECT_DATASOURCE]: function ({commit}, para) {
      return api.project.updateProjectDatasource(para)
    },
    [types.RESET_PROJECT_CONFIG]: function ({ commit }, para) {
      return api.project.resetConfig(para)
    },
    [types.UPDATE_DEFAULT_DB_SETTINGS]: function ({ commit }, para) {
      return api.project.updateDefaultDBSettings(para)
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
