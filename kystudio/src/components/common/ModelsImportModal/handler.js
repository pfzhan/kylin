export const conflictTypes = {
  DUPLICATE_MODEL_NAME: 'DUPLICATE_MODEL_NAME',
  TABLE_NOT_EXISTED: 'TABLE_NOT_EXISTED',
  COLUMN_NOT_EXISTED: 'COLUMN_NOT_EXISTED',
  INVALID_COLUMN_DATATYPE: 'INVALID_COLUMN_DATATYPE'
}

export const conflictOrder = Object.keys(conflictTypes)

export const importableConflictTypes = [
]

export const brokenConflictTypes = [
  conflictTypes.DUPLICATE_MODEL_NAME,
  conflictTypes.TABLE_NOT_EXISTED,
  conflictTypes.COLUMN_NOT_EXISTED,
  conflictTypes.INVALID_COLUMN_DATATYPE
]

export function formatConflictsGroupByName (conflicts = []) {
  const conflictsGroupsMap = {}

  for (const conflict of conflicts) {
    if (conflictsGroupsMap[conflict.type]) {
      conflictsGroupsMap[conflict.type].push(conflict.items)
    } else {
      conflictsGroupsMap[conflict.type] = [conflict.items]
    }
  }

  const result = Object.entries(conflictsGroupsMap)
    .sort(([typeA], [typeB]) => conflictOrder.indexOf(typeA) < conflictOrder.indexOf(typeB) ? -1 : 1)
    .map(([type, conflictArray]) => ({ type, conflicts: conflictArray }))
  return result
}
