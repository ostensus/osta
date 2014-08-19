var mysql = require('mysql');
var squel = require("squel");
var doT   = require('dot');

squel.useFlavour('mysql');

var idTemplate        = doT.template("lhs.{{= it.id }}");
var joinTemplate      = doT.template("lhs.{{= it.id }} >= rhs.{{= it.id }}");
var partitionTemplate = doT.template("CAST(DATE_FORMAT(lhs.{{= it.partition }}, '%Y-%m-%d') as DATETIME)");

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

    var target = {id: "imsi", partition: "timestamp"};

    var aligned = squel.select().
      field(idTemplate(target), "id").
      field(partitionTemplate(target), "day").
      field("MD5(CONCAT(lhs.called_number, lhs.calling_number))", "version"). // Need to parameterize
      field("CAST(ceil((CAST(COUNT(*) AS decimal) / 100)) AS UNSIGNED)", "bucket").
      from("call_records", "lhs").
      join("call_records", "rhs", joinTemplate(target)).
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

    var daily = squel.select().
      field("day").
      field("MD5(GROUP_CONCAT(digest ORDER BY bucket ASC separator ''))", "digest").
      from(bucketed, "bucketed").
      group("day").
      order("day", true);

    var monthly = squel.select().
      field("CAST(DATE_FORMAT(day, '%Y-%m-01') as DATETIME)", "month").
      field("MD5(GROUP_CONCAT(digest ORDER BY day ASC separator ''))", "digest").
      from(daily, "daily").
      group("month").
      order("month", true);

    var yearly = squel.select().
      field("CAST(DATE_FORMAT(month, '%Y-01-01') as DATETIME)", "year").
      field("MD5(GROUP_CONCAT(digest ORDER BY month ASC separator ''))", "digest").
      from(monthly, "monthly").
      group("year").
      order("year", true);

    var query = connection.query(yearly.toString(), function(err, rows, fields) {
        if (err) {
          callback(err);
        } else { 
          //console.log(rows);         
          callback(null, rows);  
        }
    });

    console.log("Partitioning query: " + query.sql);

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