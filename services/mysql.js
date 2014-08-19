var mysql = require('mysql');
var squel = require("squel");

var OstaMysql = function() {
	var self = this;
  var connection = mysql.createConnection({
      host     : 'localhost',
      database : 'osta',
      user     : 'osta',
      password : 'osta'
    });

  self.partition = function(callback) { 

    connection.connect();

    var sql = squel.select().from("call_records").toString();

    connection.query(sql, function(err, rows, fields) {
        if (err) {
          callback(err);
        } else {          
          callback(null, rows);  
        }
    });

    connection.end();
  },

	self.smokeTest = function(callback) {	

		connection.connect();

		connection.query('SELECT 1 + 1 AS two', function(err, rows, fields) {
		  	if (err) {
		  		callback(err);
		  	} else {		  		
		  		callback(null, rows[0].two);	
		  	}
		});

		connection.end();
	}
}

module.exports = OstaMysql;