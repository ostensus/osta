var mysql = require('mysql');
var squel = require("squel");
var doT   = require('dot');

var TemplateBuilder  = require("../lib/template.js"); 

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

    var metadata = {
      dbms:      "mysql", 
      table:     "call_records",
      id:        { name: "imsi" },
      version:   [ 
        { name: "called_number" },
        { name: "calling_number" }
      ],
      partition: { name: "timestamp" }
    };

    var builder = new TemplateBuilder();
    var template = builder.buildTemplate(metadata);

    console.log("Generated SQL template: " + template);

    connection.connect();

    var query = connection.query(template, function(err, rows, fields) {
        if (err) {
          callback(err);
        } else { 
          //console.log(rows);         
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