var http = require('http');
var url = require('url');

var seeds_file_path_arg = process.argv.slice(2);
var seeds_file = require('fs');
var seeds_file_path = '/etc/dynomite/seeds.list';

if(typeof seeds_file_path_arg == 'undefined' || seeds_file_path_arg == null || seeds_file_path_arg  == ''){
  seeds_file_path = '/etc/dynomite/seeds.list';
} else{
  seeds_file_path = seeds_file_path_arg;
}

var server = http.createServer(function(req, res) {
  
  var path = url.parse(req.url).pathname;
  console.log("Request: "+ path);


  res.writeHead(200, {"Content-Type": "application/json"});
  if (path == '/REST/v1/admin/get_seeds') {
    data = seeds_file.readFileSync(seeds_file_path).toString();
    data_oneline = data.trim().replace(/\n/g, '|');
    var now = new Date();
    var jsonDate = now.toJSON();
    console.log(jsonDate + " - get_seeds [" + data_oneline + "]");
    res.write(data_oneline);
  }
  res.end();
});
server.listen(8080);
