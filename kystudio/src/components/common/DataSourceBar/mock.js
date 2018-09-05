export function getTableMockTags () {
  const seed = Math.random()

  if (seed > 0 && seed < 0.2) {
    return ['N']
  } else if (seed > 0.2 && seed < 0.6) {
    return ['F']
  } else if (seed > 0.6 && seed < 1) {
    return ['L']
  }
}

export function getColumnMockTags () {
  const seed = Math.random()

  if (seed > 0 && seed < (1 / 4)) {
    return []
  } else if (seed > (1 / 4) && seed < (2 / 4)) {
    return ['D']
  } else if (seed > (2 / 4) && seed < (3 / 4)) {
    return ['D', 'FK']
  } else if (seed > (3 / 4) && seed < (4 / 4)) {
    return ['M']
  }
}
