#!/usr/bin/env node

var inquirer = require("inquirer");
var yargs    = require("yargs");
var _        = require("underscore");

var SqliteTemplate  = require("../lib/sqlite.js");
var TemplateBuilder = require("../lib/template.js");

yargs.usage("Produces an SQL template to introspect your database");
yargs.alias("h", "help");

yargs.describe("sqlite", "Path to sqlite file");
yargs.example("$0 --sqlite FILENAME", "Introspect the sqlite database given by this file path")

var argv = yargs.argv

if (argv.h || argv.help) {
  console.log(yargs.help());
  process.exit(1);
}


var dbTypeQuestion = [
	{
    type: "list",
    name: "type",
    message: "What type of DB do you want to inspect?",
    choices: [ "sqlite" ]
  }
];

var sqliteQuestions = [
  {
    type: "input",
    name: "path",
    default: argv.sqlite,
    message: "Path to sqlite DB"
  }
];

var tableChoice = 
  {
    type: "list",
    name: "table",
    message: "Which table would you like to generate a template for?"
  };

var columnChoice = [
  {
    type: "list",
    name: "id",
    message: "Which is the id column?"
  },
  {
    type: "list",
    name: "version",
    message: "Which is the version column?"
  },
  {
    type: "list",
    name: "partition",
    message: "Which is the partition column?"
  }
];

if (argv.sqlite) {

  inquirer.prompt( sqliteQuestions, function( sqliteAnswers ) {    
      
      var sqlite = new SqliteTemplate();
      sqlite.listTables(argv.sqlite, function(err, result) {
        var tables = _.map(result, function(x) { return x.name; });
        if (tables.length == 0) {
          console.log("No tables found");
          return;
        }

        tableChoice.choices = tables;
        inquirer.prompt( tableChoice, function( tableAnswer ) {

          sqlite.listColumns(argv.sqlite, tableAnswer.table, function(err, result) {
            // console.log("Cols: " + JSON.stringify(result, null, "  ") );

            var columns = _.object(_.map(result, function(x) {
               return [x.name, x];
            }));

            var availableColumns = _.keys(columns);
            var preener = function(x) { availableColumns.splice( availableColumns.indexOf(x), 1); return x; };

            columnChoice[0].choices = availableColumns;
            columnChoice[0].filter = preener;
            columnChoice[1].choices = availableColumns;
            columnChoice[1].filter = preener;
            columnChoice[2].choices = availableColumns;
            
            inquirer.prompt( columnChoice, function( columnsAnswers ) {

              var metadata = {
                table:     tableAnswer.table,
                id:        columns[columnsAnswers["id"]], 
                version:   columns[columnsAnswers["version"]], 
                partition: columns[columnsAnswers["partition"]]
              };

              console.log("metadata: " + JSON.stringify(metadata, null, "  ") );

              var builder = new TemplateBuilder();
              var template = builder.buildTemplate(metadata);

              console.log("template: " + template );

              sqlite.verify(argv.sqlite, template, function(err, result) {
                if (err) {
                  console.log(err);
                } else {
                  console.log("result: " + JSON.stringify(result, null, "  "));  
                }
                
              });

            });
          });

        });

      });

  });

} else {
  
  inquirer.prompt( dbTypeQuestion, function( dbTypeAnswer ) {
    console.log( JSON.stringify(dbTypeAnswer, null, "  ") );
  });  

}
