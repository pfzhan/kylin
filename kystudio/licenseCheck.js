var fs = require("fs");
var path = require("path"); //解析需要遍历的文件夹
var filePath = path.resolve("./node_modules");

// 如果checkresult.txt生成文件已存在先删除文件
fs.existsSync(path.resolve('./checkresult.txt')) && fs.unlinkSync(path.resolve('./checkresult.txt'), function(err) {
  if (err) throw err
})
//调用文件遍历方法
fileDisplay(filePath);
let result = {};
let count;
//文件遍历方法
function fileDisplay(filePath) {
  //根据文件路径读取文件，返回文件列表
  fs.readdir(filePath, function(err, files) {
    if (err) {
      console.warn(err);
    } else {
      //遍历读取到的文件列表
      files.forEach(function(filename) {
        //获取当前文件的绝对路径
        var filedir = path.join(filePath, filename);
        //根据文件路径获取文件信息，返回一个fs.Stats对象
        fs.stat(filedir, function(eror, stats) {
          if (eror) {
            console.warn("获取文件stats失败");
          } else {
            var isFile = stats.isFile(); //是文件
            var isDir = stats.isDirectory(); //是文件夹
            if (isFile) {
              if (filename === "package.json") {
                // 读取文件内容
                var content = fs.readFileSync(filedir, "utf-8");
                let s = JSON.parse(content);
                if ((s._id || s.name) && !result[s._id || s.name]) {
                  result[s._id || s.name] = s.license;
                  count++;
                  let license;
                  let writeContent = (s._id || s.name) + ":";
                  let packageLicense = s.license || s.licenses
                  if (Object.prototype.toString.call(packageLicense) === '[object Object]') {
                    license = packageLicense.type;
                  } else if (Object.prototype.toString.call(packageLicense) === '[object Array]') {
                    license = packageLicense[0].type;
                  } else if (!packageLicense) {
                    if (fs.existsSync(path.resolve(filePath + '/LICENSE'))) {
                      let cont = fs.readFileSync(path.resolve(filePath + '/LICENSE'), 'utf-8')
                      license = cont.split('\n')[0]
                    }
                  } else {
                    license = packageLicense;
                  }
                  if (license === 'GPL') {
                    throw new Error("[三方依赖包license为 GPL]：", filedir)
                  }
                  writeContent += license;
                  writeContent += "\r";
                  fs.writeFile(
                    "./checkresult.txt",
                    writeContent,
                    { flag: "a" },
                    function(err) {
                      if (err) {
                        throw err;
                      }
                    }
                  );
                }
              }
            }
            if (isDir) {
              fileDisplay(filedir); //递归，如果是文件夹，就继续遍历该文件夹下面的文件
            }
          }
        });
      });
    }
  });
}
