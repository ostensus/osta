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

    var aligned = squel.select().
      field("lhs.imsi", "id").
      field("CAST(DATE_FORMAT(lhs.timestamp, '%Y-%m-%d') as DATETIME)", "day").
      field("MD5(CONCAT(lhs.called_number, lhs.calling_number))", "version").
      field("CAST(ceil((CAST(COUNT(*) AS decimal) / 100)) AS INT)", "bucket").
      from("call_records", "lhs").
      join("call_records", "rhs", "lhs.imsi >= rhs.imsi").
      group("day", "id", "version").
      order("day", true).
      order("bucket", true);

    var bucketed = squel.select().
      field("day").
      field("bucket").
      field("MD5(GROUP_CONCAT(version ORDER BY id ASC separator ''))", "digest").
      from(aligned, "aligned").
      group("day", "bucket").
      order("day", true).
      order("bucket", true);

    var query = connection.query(bucketed.toString(), function(err, rows, fields) {
        if (err) {
          callback(err);
        } else { 
          console.log(rows[0]);         
          callback(null, rows);  
        }
    });

    //console.log("Partitioning query: " + query.sql);

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