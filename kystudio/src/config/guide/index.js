import { addProjectDrama } from './addProject'
import { loadTableDrama } from './loadTable'
import { addModelDrama } from './addModel'
import { monitorDrama } from './monitor'
import { addIndexDrama } from './addIndex'
import { queryDrama } from './query'
import { accelerationDrama } from './acceleration'
import { tableSettingDrama } from './tableSetting'
export const drama = {
  // 手动演示流程
  manual_1: () => [...addProjectDrama(), ...loadTableDrama(), ...addModelDrama(), ...addIndexDrama()],
  manual: () => [...addProjectDrama(), ...loadTableDrama()],
  project: () => [...addProjectDrama()],
  autoProject: () => [...addProjectDrama('auto')],
  autoLoadTable: () => [...loadTableDrama(), ...tableSettingDrama()],
  loadTable: () => [...loadTableDrama()],
  addModel: () => [...addModelDrama(), ...addIndexDrama()],
  monitor: () => [...monitorDrama()],
  query: () => [...queryDrama()],
  acceleration: () => [...accelerationDrama()]
}

