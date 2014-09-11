var assert     = require("assert");
var postgrator = require('postgrator');
var mysql      = require('mysql');
var _          = require('underscore');
var Chance     = require('chance');
var moment     = require('moment');

var chance = new Chance();

var client = mysql.createConnection({
  	host: 	  '127.0.0.1',
  	user:     'osta',
  	password: 'osta',
  	database: 'osta'
});

var OstaMysql  = require('../services/mysql');
var Ostn       = require('../lib/ostn');

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

    client.connect();

    client.query("TRUNCATE TABLE call_records;", function(err, res) {
      if (err) done(err);
    });

    var start = moment("2012-08-03 22:21:31");

    var calls = _.range(1000).map(function (n) { 
      var imsi = 230023741299234 + chance.integer({min: 0, max: 2}); + '';
      var duration = chance.integer({min: 1, max: 100});
      var timestamp = start.add(1, 'days').format("YYYY-MM-DD HH:mm:ss");
      var caller = chance.phone();
      var callee = chance.phone();
      return [imsi, timestamp, duration, caller, callee];
    });

    var sql = "INSERT INTO call_records (imsi, timestamp, duration, calling_number, called_number) VALUES ?";

    client.query(sql, [calls], function(err) {
        done(err);
    });

    client.end();
  });

})

test('Osta Mysql smoke test', function(done){
  var osta = new OstaMysql();
  
  osta.smokeTest(function(err, result) {
   done(err);
   assert.equal(2, result);
  });
  
});

test('Repo smoke test', function(done){
  var ostn = new Ostn();
  
  ostn.newRepo("quux", function(err, result) {
    done(err);
    assert(result > 0, "Repo Id was " + result);
  });
});

// test('Interview smoke test', function(done){
//   var interview = new Interview();

//   // $ echo -n "quux" | md5
//   // 9fb045bba26522b2a50bb3e7a06ae30d

//   interview.nextQuestion("quux", function(err, result) {
//     done(err);
//     assert.equal("9fb045bba26522b2a50bb3e7a06ae30d", result);
//   });
// });

test('Osta Mysql partition test', function(done){
  var osta = new OstaMysql();
  
  osta.partition(function(err, result) {
    done(err);
    assert.equal(4, result.length); // 1000 days should span 4 years
  });
  
});
