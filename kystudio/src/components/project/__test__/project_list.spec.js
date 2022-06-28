import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import { localVue } from '../../../../test/common/spec_common'
import ProjectList from '../project_list.vue'
import * as business from '../../../util/business'
import * as utils from '../../../util/index'
import { projectList } from './mock'

const handleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback, errorCallback) => {
  callback(res)
  errorCallback && errorCallback()
})
const handleError = jest.spyOn(business, 'handleError').mockRejectedValue(false)
const handleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation((res) => {
  return Promise.resolve(res)
})
const kapConfirm = jest.spyOn(business, 'kapConfirm').mockResolvedValue(true)
const mockMessageSuccess = jest.fn()
const mockMessage = jest.fn().mockImplementation()

const mockApi = {
  mockLoadProjectList: jest.fn().mockImplementation(({state}) => {
    state.project.projectList = projectList
    state.project.projectTotalSize = projectList.length
    return Promise.resolve(projectList)
  }),
  mockLoadAllProject: jest.fn().mockImplementation(),
  mockDeleteProject: jest.fn().mockResolvedValue(true),
  mockUpdateProject: jest.fn().mockImplementation(),
  mockSaveProject: jest.fn().mockImplementation(),
  mockBackupProject: jest.fn().mockResolvedValue(true),
  mockGetAvailableProjectOwners: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      limit: 0,
      offset: 0,
      total_size: 0,
      value: []
    })
  }),
  mockUpdateProjectOwner: jest.fn().mockResolvedValue(true),
  mockCallProjectEditModal: jest.fn().mockResolvedValue(true),
  mockCallModelsExportModal: jest.fn().mockImplementation(),
  mockCallModelsImportModal: jest.fn().mockImplementation(),
  mockResetQueryTabs: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    project: {
      projectList: [],
      projectTotalSize: 0
    },
    user: {
      currentUser: {
        authorities: [{authority: "ROLE_ADMIN"}],
        create_time: 1611714654039,
        defaultPassword: true,
        disabled: false,
        first_login_failed_time: 0,
        last_modified: 1614754512000,
        locked: false,
        locked_time: 0,
        mvcc: 0,
        username: "ADMIN",
        uuid: "4c22ffde-7731-4a02-89e5-6fc9d661b97a",
        version: "4.0.0.0",
        wrong_time: 0
      }
    }
  },
  actions: {
    'LOAD_PROJECT_LIST': mockApi.mockLoadProjectList,
    'LOAD_ALL_PROJECT': mockApi.mockLoadAllProject,
    'DELETE_PROJECT': mockApi.mockDeleteProject,
    'UPDATE_PROJECT': mockApi.mockUpdateProject,
    'SAVE_PROJECT': mockApi.mockSaveProject,
    'BACKUP_PROJECT': mockApi.mockBackupProject,
    'GET_AVAILABLE_PROJECT_OWNERS': mockApi.mockGetAvailableProjectOwners,
    'UPDATE_PROJECT_OWNER': mockApi.mockUpdateProjectOwner
  },
  mutations: {
    'RESET_QUERY_TABS': mockApi.mockResetQueryTabs
  },
  getters: {
    projectActions: () => {
      return ['addProject', 'accessActions', 'changeProjectOwner', 'deleteProject', 'executeModelsMetadata']
    }
  },
  modules: {
    'ProjectEditModal': {
      namespaced: true,
      actions: {
        'CALL_MODAL': mockApi.mockCallProjectEditModal
      }
    },
    'ModelsExportModal': {
      namespaced: true,
      actions: {
        'CALL_MODAL': mockApi.mockCallModelsExportModal
      }
    },
    'ModelsImportModal': {
      namespaced: true,
      actions: {
        'CALL_MODAL': mockApi.mockCallModelsImportModal
      }
    }
  }
})

const wrapper = shallowMount(ProjectList, {
  store,
  localVue,
  mocks: {
    handleSuccess,
    handleError,
    handleSuccessAsync,
    kapConfirm,
    $message: mockMessage
  }
})
wrapper.vm.$message.prototype.success = mockMessageSuccess

describe('Component ProjectList', () => {
  it('init', () => {
    expect(mockApi.mockLoadProjectList).toBeCalled()
  })
  it('computed', async () => {
    expect(wrapper.vm.projectList).toEqual(projectList)
    expect(wrapper.vm.projectsTotal).toBe(1)
    expect(wrapper.vm.isAdmin).toBeTruthy()
    expect(wrapper.vm.emptyText).toBe('No data')
    await wrapper.setData({filterData: {...wrapper.vm.filterData, project: 'test'}})
    expect(wrapper.vm.emptyText).toBe('No Results')
  })
  it('methods', async () => {
    expect(wrapper.vm.canExecuteModelMetadata()).toBeTruthy()

    wrapper.vm.inputFilter('')
    expect(wrapper.vm.filterData.project).toBe('')
    expect(wrapper.vm.filterData.page_offset).toBe(0)
    expect(mockApi.mockLoadProjectList).toBeCalled()

    wrapper.vm.$refs = {
      projectForm: {
        $emit: jest.fn()
      }
    }
    wrapper.vm.checkProjectForm()
    expect(wrapper.vm.$refs.projectForm.$emit).toBeCalledWith('projectFormValid')

    wrapper.vm.handleCurrentChange(2, 10)
    expect(wrapper.vm.filterData.page_offset).toBe(2)
    expect(wrapper.vm.filterData.page_size).toBe(10)
    expect(mockApi.mockLoadProjectList.mock.calls[2][1]).toEqual({"exact": false, "page_offset": 2, "page_size": 10, "permission": "ADMINISTRATION", "project": ""})

    await wrapper.vm.newProject()
    expect(mockApi.mockCallProjectEditModal.mock.calls[0][1]).toEqual({"editType": "new"})
    expect(mockApi.mockLoadProjectList.mock.calls[3][1]).toEqual({"exact": false, "page_offset": 2, "page_size": 10, "permission": "ADMINISTRATION", "project": ""})
    expect(mockApi.mockLoadAllProject).toBeCalled()

    await wrapper.vm.changeProject('test')
    expect(mockApi.mockCallProjectEditModal.mock.calls[1][1]).toEqual({"editType": "edit", "project": "test"})
    expect(mockApi.mockLoadProjectList.mock.calls[4][1]).toEqual({"exact": false, "page_offset": 2, "page_size": 10, "permission": "ADMINISTRATION", "project": ""})
    expect(mockApi.mockLoadAllProject).toBeCalled()

    await wrapper.vm.removeProject({name: 'xm_test'})
    await wrapper.vm.$nextTick()
    expect(kapConfirm.mock.calls[0]).toEqual(["The project \"xm_test\" cannot be restored after deletion. Are you sure you want to delete?", null, "Delete Project"])
    expect(mockApi.mockDeleteProject.mock.calls[0][1]).toBe('xm_test')
    expect(mockMessage).toBeCalledWith({"message": "Deleted successfully.", "type": "success"})
    expect(mockApi.mockLoadProjectList.mock.calls[5][1]).toEqual({"exact": false, "page_offset": 2, "page_size": 10, "permission": "ADMINISTRATION", "project": ""})
    expect(mockApi.mockLoadAllProject).toBeCalled()

    wrapper.vm.$store._actions.DELETE_PROJECT = [jest.fn().mockImplementation(() => {
      return {
        then: (successCallback, errorCallback) => {
          errorCallback && errorCallback()
        }
      }
    })]
    await wrapper.vm.removeProject({name: 'xm_test'})
    expect(handleError).toBeCalled()

    await wrapper.vm.handleExportModels({name: 'xm_test'})
    expect(mockApi.mockCallModelsExportModal.mock.calls[0][1]).toEqual({"project": "xm_test", "type": "all"})

    await wrapper.vm.handleImportModels({name: 'xm_test'})
    expect(mockApi.mockCallModelsImportModal.mock.calls[0][1]).toEqual({"project": "xm_test"})

    wrapper.vm.backup('xm_test')
    await wrapper.vm.$nextTick()
    expect(kapConfirm.mock.calls[2]).toEqual(["Are you sure you want to backup this project ?", {"type": "info"}, "Backup Project"])
    expect(mockApi.mockBackupProject.mock.calls[0][1]).toEqual('xm_test')
    expect(handleSuccess).toBeCalled()
    expect(mockMessage.mock.calls[1]).toEqual([{"message": "Metadata backup successfully: true", "type": "success"}])

    wrapper.vm.$store._actions.BACKUP_PROJECT = [jest.fn().mockImplementation(() => {
      return {
        then: (successCallback, errorCallback) => {
          errorCallback && errorCallback()
        }
      }
    })]
    await wrapper.vm.backup({name: 'xm_test'})
    expect(handleError).toBeCalled()

    expect(wrapper.vm.initAccessMeta()).toEqual({permission: '', principal: true, sid: ''})
    expect(wrapper.vm.hasAdminProjectPermission()).toBe(null)

    await wrapper.vm.loadAvailableProjectOwners('test')
    expect(wrapper.vm.ownerFilter.name).toBe('test')
    expect(mockApi.mockGetAvailableProjectOwners.mock.calls[0][1]).toEqual({"name": "test", "page_offset": 0, "page_size": 100, "project": ""})
    expect(handleSuccessAsync).toBeCalled()
    expect(wrapper.vm.userOptions).toEqual([])

    wrapper.vm.$store._actions.GET_AVAILABLE_PROJECT_OWNERS = [jest.fn().mockRejectedValue({body: {msg: 'error'}})]
    await wrapper.vm.loadAvailableProjectOwners('')
    expect(wrapper.vm.ownerFilter.name).toBe('')
    expect(mockMessage.mock.calls[2]).toEqual([{"closeOtherMessages": true, "message": "error", "type": "error"}])

    await wrapper.vm.openChangeProjectOwner('xm_test')
    expect(wrapper.vm.projectOwner.project).toBe('xm_test')
    expect(wrapper.vm.ownerFilter.project).toBe('xm_test')
    expect(wrapper.vm.changeOwnerVisible).toBeTruthy()

    expect(await wrapper.vm.changeProjectOwner()).toBe()

    await wrapper.setData({projectOwner: {...wrapper.vm.projectOwner, owner: 'ADMIN'}})
    await wrapper.vm.changeProjectOwner()
    expect(mockApi.mockUpdateProjectOwner.mock.calls[0][1]).toEqual({"owner": "ADMIN", "project": "xm_test"})
    expect(wrapper.vm.changeLoading).toBeFalsy()
    expect(wrapper.vm.changeOwnerVisible).toBeFalsy()
    expect(mockMessage.mock.calls[3]).toEqual([{"closeOtherMessages": true, "message": "error", "type": "error"}])

    wrapper.vm.$store._actions.UPDATE_PROJECT_OWNER = [jest.fn().mockRejectedValue({body: {msg: 'error'}})]
    await wrapper.vm.changeProjectOwner()
    expect(wrapper.vm.changeLoading).toBeFalsy()
    expect(wrapper.vm.changeOwnerVisible).toBeFalsy()
    expect(mockMessage.mock.calls[4]).toEqual([{"message": "The owner of the project \"xm_test\" has been successfully changed to \"ADMIN\".", "type": "success"}])
  })
})

