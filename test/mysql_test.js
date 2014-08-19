var assert     = require("assert");
var postgrator = require('postgrator');
var mysql      = require('mysql');
var _          = require('underscore');
var Chance     = require('chance');

var chance = new Chance();

var client = mysql.createConnection({
  	host: 	  '127.0.0.1',
  	user:     'osta',
  	password: 'osta',
  	database: 'osta'
});

var OstaMysql  = require('../services/mysql');

before(function(done){

  postgrator.config.set({
    migrationDirectory: __dirname + '/migrations',
    driver: 'mysql',
    host: '127.0.0.1',
    database: 'osta',
    username: 'osta',
    password: 'osta'
  });

  postgrator.migrate('1', function (err, migrations) {
    if (err) done(err);
  });

  client.connect();

  client.query("TRUNCATE TABLE call_records;", function(err, res) {
  	if (err) done(err);
  });

  var calls = _.range(10).map(function (n) { 
    var imsi = 230023741299234 + n + '';
    var duration = chance.integer({min: 1, max: 100});
    var timestamp = chance.date({year: 2014});
    var caller = chance.phone();
    var callee = chance.phone();
    return [imsi, timestamp, duration, caller, callee];
  });

  var sql = "INSERT INTO call_records (imsi, timestamp, duration, calling_number, called_number) VALUES ?";

  client.query(sql, [calls], function(err) {
      done(err);
  });

  client.end();

})

test('Osta Mysql smoke test', function(done){
  var osta = new OstaMysql();
  
  osta.smokeTest(function(err, result) {
  	done(err);
  	assert.equal(2, result);
  });
  
});
