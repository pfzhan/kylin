const fs = require('fs');
const path = require('path');
const npmPath = process.env.PWD;

const configs = parseArgConfig([
  '--rootPath'
])

let cacheResult = null;
const translateMap = {};

travellingFiles(path.resolve(npmPath, configs.rootPath), (filePath) => {
  process.stdout.write(`Start translating ${filePath}... `);
  // 翻译文件，输出翻译后的object
  const tranlsateResult = translateFile(filePath);
  // 非.vue和.js的文件会输出undefined，所以过滤掉
  if (tranlsateResult !== undefined) {
    translateMap[filePath] = tranlsateResult;
  }
  process.stdout.write(`Done.\n`);
});

process.stdout.write(`Writing translating file... `);
fs.writeFileSync(path.resolve(npmPath, './message.json'), JSON.stringify(translateMap, null, 2));
process.stdout.write(`Done.\n`);





// Functions
function parseArgConfig(argumentKeys = []) {
  let config = {};

  for (const key of argumentKeys) {
    const keyIndex = process.argv.findIndex(arg => arg === key);
    const value = process.argv[keyIndex + 1];
    config = { ...config, [key.replace('--', '')]: value };
  }
  return config;
}

function travellingFiles(filePath, callback) {
  const stat = fs.statSync(filePath);

  if (stat.isDirectory()) {
    const children = fs.readdirSync(filePath);
    for (const child of children) {
      travellingFiles(path.resolve(filePath, child), callback);
    }
  } else if (stat.isFile()) {
    callback(filePath);
  }
}

function translateFile(filePath) {
  switch (path.extname(filePath)) {
    case '.vue':
      return readVueTranslate(filePath);
    case '.js':
      return readJSTranslate(filePath);
    default:
      return undefined;
  }
}

function readVueTranslate(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const scriptRegex = /<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi;
  const scriptContent = content.match(scriptRegex) && content.match(scriptRegex)[0];

  if (scriptContent && /locales:\s*{/.test(scriptContent)) {
    return findLocalesObj(`{${scriptContent.split(/locales:\s*{/)[1]}`);
  }
  return null;
}

function readJSTranslate(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  if (
    content && (
      /"en"\s*:\s*{/.test(content) ||
      /en\s*:\s*{/.test(content) ||
      /'en'\s*:\s*{/.test(content)
    )
  ) {
    return findLocalesObj(`{${content.split(/export\s*default\s*{/)[1]}`);
  }
  return null;
}

function removeLastChar(string = '', char = '') {
  const lastIndex = string.lastIndexOf(char);
  return lastIndex !== -1 ? string.slice(0, lastIndex - 1) : '';
}

function findLocalesObj(string = '') {
  try {
    eval(`cacheResult = ${string};`);
    return cacheResult;
  } catch {
    const removedString = removeLastChar(string, '}');
    return removedString ? findLocalesObj(removedString) : null;
  }
}
