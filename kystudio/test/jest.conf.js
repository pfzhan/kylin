const path = require('path')

module.exports = {
  verbose: true,
  testURL: 'http://localhost/',
  rootDir: path.resolve(__dirname, '../'),
  moduleFileExtensions: [
    'js',
    'json',
    'vue'
  ],
  roots: [
    '<rootDir>/src/'
  ],
  transform: {
    '^.+\\.(vue)$': '<rootDir>/node_modules/vue-jest',
    '^.+\\.jsx?$': '<rootDir>/node_modules/babel-jest',
    '^.+\\.(css|styl|less|sass|scss|svg|png|jpg|ttf|woff|woff2)$': 'jest-transform-stub'
  },
  transformIgnorePatterns: ['node_modules'],
  // 所需忽略的文件
  testPathIgnorePatterns: ['<rootDir>/src/config'],
  setupFiles: ['<rootDir>/test/setup'],
  moduleNameMapper: {
    '^vue$': 'vue/dist/vue.common.js',
    '^src': '<rootDir>/src',
    '^assets': '<rootDir>/src/assets',
    '^components': '<rootDir>/src/components',
    '^lessdir': '<rootDir>/src/less',
    '^util': '<rootDir>/src/util',
    '^config': '<rootDir>/src/config'
    // '^themescss': '<rootDir>/node_modules/kyligence-ui/packages/theme-chalk/src',
    // '^kyligence-ui': '<rootDir>/node_modules/kyligence-ui'
  },
  // 遇到有出错的测试用例就不再执行下去
  bail: true,
  collectCoverage: true,
  collectCoverageFrom: [
    'src/canvas/*.{js,vue}',
    'src/filter/*.{js,vue}',
    'src/directive/*.{js,vue}',
    'src/util/*.{js,vue}',
    'src/components/**/*.{js,vue}',
    '!src/components/dome.vue',
    '!**/node_modules/**',
    '!src/router/**',
    '!src/locale/**',
    '!src/config/**',
    '!src/assets/**',
    '!src/service/**',
    '!src/store/**'
  ],
  // coverageReporters: ['json', 'lcovonly', 'text', 'clover'],
  coverageDirectory: './test/coverage',
  // 自定义覆盖率标准
  coverageThreshold: {
    'global': {
      'branches': 3,
      'functions': 3,
      'lines': 3,
      'statements': 3
    }
  },
  forceCoverageMatch: ['src/**/__test__/*.spec.js']
}
