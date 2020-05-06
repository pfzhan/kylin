const fs = require('fs')
const path = require('path')
const dotenv = require('dotenv')

const e2ePath = path.resolve('./specs/e2e.env')
const e2ePathLocal = path.resolve('./specs/e2e.env.local')

if (fs.existsSync(e2ePath)) {
  dotenv.config({ path: e2ePath })
}

if (fs.existsSync(e2ePathLocal)) {
  dotenv.config({ path: e2ePathLocal })
}

require('./systemAdmin/login/login.spec')
require('./systemAdmin/logout/logout.spec')
