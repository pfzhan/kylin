const path = require('path')

module.exports = {
  verbose: true,
  testURL: "http://localhost/",
  rootDir: path.resolve(__dirname, '../'),
  moduleFileExtensions: [
    'js',
    'json',
    'vue'
  ],
  roots: [
    "<rootDir>/src/"
  ],
  transform: {
    '.*\\.(vue)$': '<rootDir>/node_modules/vue-jest',
    "^.+\\.js$": "<rootDir>/node_modules/babel-jest"
  },
  transformIgnorePatterns: ['node_modules'],
  // 所需忽略的文件
  testPathIgnorePatterns: ['<rootDir>/src/config'],
  setupFiles: ['<rootDir>/test/setup'],
  moduleNameMapper: {
    'vue$': 'vue/dist/vue.common.js',
    '^src': '<rootDir>/src',
    '^assets': '<rootDir>/src/assets',
    '^components': '<rootDir>/src/components',
    '^lessdir': '<rootDir>/src/less',
    '^util': '<rootDir>/src/util',
    '^config': '<rootDir>/src/config',
    // '^themescss': '<rootDir>/node_modules/kyligence-ui/packages/theme-chalk/src',
    // '^kyligence-ui': '<rootDir>/node_modules/kyligence-ui'
  },
  // 遇到有出错的测试用例就不再执行下去
  bail: true,
  collectCoverage: true,
  collectCoverageFrom: [
    'src/filter/*.{js,vue}',
    'src/util/business.js',
    'src/util/index.js',
    'src/util/validate.js',
    'src/util/object.js',
    '!**/node_modules/**',
    '!src/**/router/**',
    '!src/config/**'
  ],
  // coverageReporters: ['json', 'lcovonly', 'text', 'clover'],
  coverageDirectory: './test/coverage',
  // 自定义覆盖率标准
  coverageThreshold: {
    'src/util/*.js': {
      branches: 5,
      functions: 5,
      lines: 5,
      statements: 5
    },
    'src/filter/*.js': {
      branches: 90,
      functions: 90,
      lines: 90,
      statements: 90
    }
  },
  forceCoverageMatch: ['src/**/__test__/*.spec.js']
}
