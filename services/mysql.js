var mysql      = require('mysql');

var OstaMysql = function() {
	var self = this;
	self.smokeTest = function(callback) {
		var connection = mysql.createConnection({
		  host     : 'localhost',
		  database : 'osta',
		  user     : 'osta',
		  password : 'osta'
		});

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