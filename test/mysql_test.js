var assert = require("assert")

var OstaMysql = require('../services/mysql');

test('Osta Mysql smoke test', function(done){
  var osta = new OstaMysql();
  
  osta.smokeTest(function(err, result) {
  	done(err);
  	assert.equal(2, result);
  });
  
});
