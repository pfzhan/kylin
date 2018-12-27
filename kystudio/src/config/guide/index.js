import { addProjectDrama, projectGroup } from './addProject'
import { loadTableDrama } from './loadTable'
import { addModelDrama } from './addModel'
import { monitorDrama } from './monitor'
import { addIndexDrama } from './addIndex'
export const drama = {
  // 手动演示流程
  manual_1: () => [...addProjectDrama(), ...loadTableDrama(), ...addModelDrama(), ...addIndexDrama()],
  manual: () => [...addProjectDrama(), ...loadTableDrama()],
  project: () => [...addProjectDrama()],
  projectStepTips: () => [...projectGroup()],
  loadTable: () => [...loadTableDrama()],
  addModel: () => [...addModelDrama(), ...addIndexDrama()],
  monitor: () => [...monitorDrama()]
}

