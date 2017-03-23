export function fromObjToArr (obj) {
  let arr = []
  for (let key of Object.keys(obj)) {
    arr.push({
      key: key,
      value: obj[key]
    })
  }
  return arr
}

export function fromArrToObj (arr) {
  let obj = {}
  for (let item of arr) {
    obj[item.key] = item.value
  }
  return obj
}

export function sampleGuid () {
  let randomNumber = ('' + Math.random()).replace(/\./, '')
  return (new Date()).getTime() + '_' + randomNumber
}

export function removeNameSpace (str) {
  if (str) {
    return str.replace(/([^.\s]+\.)+/, '')
  } else {
    return ''
  }
}

export function getNameSpaceTopName (str) {
  if (str) {
    return str.replace(/(\.[^.]+)+/, '')
  } else {
    return ''
  }
}

export function getNameSpace (str) {
  if (str) {
    return str.replace(/(\.[^.]+)$/, '')
  } else {
    return ''
  }
}
