module.exports = {
  bundles: {
    首页: /\/dashboard\//,
    查询: /\/query\//,
    登录: [
      /login\.vue/,
    ],
    建模中心: [
      /\/studio\//,
      /\/Model/,
      /\/datasource\//,
      /\/DataSourceModal\//
    ],
    监控: /\/monitor\//,
    设置: /\/setting\//,
    系统管理: [
      /\/admin\//,
      /\/project\//,
      /\/security\//,
      /\/user\//,
    ],
    '顶部工具栏+导航栏': [
      /about_kap\.vue/,
      /help\.vue/,
      /\/Guide\//,
    ],
    其他: '*'
  },
};
