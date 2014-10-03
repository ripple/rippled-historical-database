var Promise = require('bluebird');
var fs      = Promise.promisifyAll(require("fs"));
var files   = ['import.config.json', 'api.config.json'];

Promise.map(files, function(filename) {
  var path = './config/' + filename;
  
  return fs.existsAsync(path)
    .then(function(){      
      console.log("Creating config file:", filename);
      fs.createReadStream(path + '.example')
        .pipe(fs.createWriteStream(path)); 
    })
    .error(function(e) {
      console.log("Config file exists:", filename);
    });
  
}).then(function(){
  console.log('Done.');
  console.log('enter the db credentials then run \'node migrate\'');
});
