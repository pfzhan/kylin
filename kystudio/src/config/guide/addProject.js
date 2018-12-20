export function addProjectDrama () {
  return [
    // 添加project
    {
      eventID: 1,
      done: false,
      target: 'addProjectBtn' // 鼠标移动到添加project按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'addProjectBtn' // 鼠标点击添加project按钮
    },
    {
      eventID: 1,
      done: false,
      target: 'addProjectInput' // 鼠标移动到添加project 名称输入框
    },
    {
      eventID: 3,
      done: false,
      target: 'addProjectInput', // 输入添加project 名称
      val: 'SSB123' + Date.now()
    },
    {
      eventID: 1,
      done: false,
      target: 'changeMunalProjectType' // 鼠标移动到选择类型
    },
    {
      eventID: 2,
      done: false,
      target: 'changeMunalProjectType' // 鼠标点击选择类型
    },
    {
      eventID: 1,
      done: false,
      target: 'addProjectDesc' // 鼠标移动到输入project 描述
    },
    {
      eventID: 3,
      done: false,
      target: 'addProjectDesc', // 输入project 描述,
      val: 'This is a test project'
    },
    {
      eventID: 1,
      done: false,
      target: 'saveProjectBtn' // 鼠标移动到project 保存按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'saveProjectBtn' // 点击project 保存按钮
    }
  ]
}
