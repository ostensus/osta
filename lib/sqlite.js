var fs      = require("fs");
var sqlite3 = require("sqlite3").verbose();

var SqliteTemplate = function() {
	var self = this;

  self.verify = function(file, sql, callback) {
    var exists = fs.existsSync(file);
    if (exists) {

      var db = new sqlite3.Database(file);
      db.serialize(function() {

        db.loadExtension("libsqlitemd5.so", function(err) {
          if (err) {
            console.log("Could not load libsqlitemd5.so: ", err);
          }
        });

        db.loadExtension("libsqlitefunctions.so", function(err) {
          if (err) {
            console.log("Could not load libsqlitefunctions.so: ", err);
          }
        });

        db.all(sql, function(err, rows) {
          if (err) {
            callback(err);
          }
          else {
            callback(null, rows);
          }
        });

      });
      db.close();

    } else {
      console.log("Could not find DB " + file); 
    }   
  },

  self.listTables = function(file, callback) {
		var exists = fs.existsSync(file);
		if (exists) {

			var db = new sqlite3.Database(file);
			db.serialize(function() {

        db.all("SELECT name FROM sqlite_master where type = 'table';", function(err, rows) {
          if (err) {
            callback(err);
          }
          else {
            callback(null, rows);
          }
        });

			});
      db.close();

		} else {
			console.log("Could not find DB " + file);	
		}	
	},

  self.listColumns = function(file, table, callback) {
    var exists = fs.existsSync(file);
    if (exists) {

      var db = new sqlite3.Database(file);
      db.serialize(function() {

        db.all("PRAGMA table_info(" + table + ");", function(err, rows) {
          if (err) {
            callback(err);
          }
          else {
            callback(null, rows);
          }
        });

      });
      db.close();

    } else {
      console.log("Could not find DB " + file); 
    } 
  }
}

module.exports = SqliteTemplate;
