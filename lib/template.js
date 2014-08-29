var squel = require("squel");
var doT   = require("dot");
var _     = require("underscore");

/////////////////////////////////

var quoting = {
  sqlite: true
};

var innermostAliases = {
  mysql: { id: "id", version: "version" },
  sqlite: { id: "lhs.id", version: "lhs.version" }
};

var dateTruncFormats = {
  mysql: { day: "%Y-%m-%d", month: "%Y-%m-01", year: "%Y-01-01" },
  sqlite: { day: "%Y-%m-%d", month: "%Y-%m-01", year: "%Y-01-01" }
};

var dateTruncFormatters = {
  mysql:  "CAST(DATE_FORMAT({{= it.name }}, '{{= it.format }}') as DATETIME)",
  sqlite: "STRFTIME('{{= it.format }}', {{= it.name }})" 
};

var md5Formatters = {
  mysql:  "MD5(GROUP_CONCAT({{= it.concat }} ORDER BY {{= it.orderBy }} ASC separator ''))",
  sqlite: "LOWER(HEX(MD5(GROUP_CONCAT({{= it.concat }},''))))" 
};

var md5ConcatFormatters = {
  mysql:  function (versionCols) { return "MD5(CONCAT(lhs." + versionCols.join(", lhs.") + "))"; },
  sqlite: function (versionCols) { return "MD5(lhs." + versionCols.join(" || lhs.") + ")"; }
};

/////////////////////////////////

var rollupColumnName = { 
  day:   function(md, q) { return "lhs." + md.partition.name; },
  month: function(md, q) { return q("day"); },
  year:  function(md, q) { return q("month"); }
};

var idTemplate        = doT.template("lhs.{{= it.id.name }}");
var joinTemplate      = doT.template("lhs.{{= it.id.name }} >= rhs.{{= it.id.name }}");

var TemplateBuilder = function() {
	var self = this;

  self.buildTemplate = function(metadata) {

    var dtTemp  = doT.template(dateTruncFormatters[metadata.dbms]);
    var md5Temp = doT.template(md5Formatters[metadata.dbms]);

    var part = function(granularity) {
      
      var quoter = function(x) { return ( quoting[metadata.dbms] ) ? "'" + x + "'" : x; };
      var formatting = {
        format: dateTruncFormats[metadata.dbms][granularity],
        name: rollupColumnName[granularity](metadata, quoter)
      };

      return dtTemp(formatting); 
    };

  	var versionColumns = _.map(metadata.version, function(v) { return v.name} );
  	
    var aligned = squel.select().
      field(idTemplate(metadata), "id");

      if (metadata.partition) {      
      	aligned.field(part("day"), "day");
      }

      aligned.
      	field(md5ConcatFormatters[metadata.dbms](versionColumns), "version"). // Very hacky join :-(
      	field("CAST(ceil((CAST(COUNT(*) AS DECIMAL) / 100)) AS UNSIGNED)", "bucket").
      	from(metadata.table, "lhs").
      	join(metadata.table, "rhs", joinTemplate(metadata));

      if (metadata.partition) {
      	aligned.
      		group("day").
      		group(innermostAliases[metadata.dbms]["id"]).
      		group(innermostAliases[metadata.dbms]["version"]).
      		order("day", true);
      } else {
      	aligned.
          group(innermostAliases[metadata.dbms]["id"]).
      		group(innermostAliases[metadata.dbms]["version"]);
	    }

      aligned.order("bucket", true);

    var bucketed = squel.select().
    	field("bucket");

    	if (metadata.partition) {
    		bucketed.field("day");	
    	}
      
      bucketed.
        field(md5Temp( { concat: "version", orderBy: "id"} ), "digest").
      	from(aligned, "aligned");

      if (metadata.partition) {
    		bucketed.
    			group("day").
    			group("bucket").
    			order("day", true);
    	} else {
    		bucketed.group("bucket");
    	}
      
      bucketed.order("bucket", true);

    if (metadata.partition) {

    	var daily = squel.select().
	      field("day").
        field(md5Temp( { concat: "digest", orderBy: "bucket"} ), "digest").
	      from(bucketed, "bucketed").
	      group("day").
	      order("day", true);

	    var monthly = squel.select().
        field(part("month"), "month").
        field(md5Temp( { concat: "digest", orderBy: "day"} ), "digest").
	      from(daily, "daily").
	      group("month").
	      order("month", true);

	    var yearly = squel.select().
        field(part("year"), "year").
        field(md5Temp( { concat: "digest", orderBy: "month"} ), "digest").
	      from(monthly, "monthly").
	      group("year").
	      order("year", true);

	    return yearly.toString();

    } else {

    	return bucketed.toString();
    }
  }
}

module.exports = TemplateBuilder;