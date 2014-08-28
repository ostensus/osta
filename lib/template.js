var squel = require("squel");
var doT   = require("dot");
var _     = require("underscore");

var idTemplate        = doT.template("lhs.{{= it.id.name }}");
var joinTemplate      = doT.template("lhs.{{= it.id.name }} >= rhs.{{= it.id.name }}");
var partitionTemplate = doT.template("STRFTIME('%Y-%m-%d',lhs.{{= it.partition.name }})");

var TemplateBuilder = function() {
	var self = this;

  self.buildTemplate = function(metadata) {

  	var versionColumns = _.map(metadata.version, function(v) { return v.name} );
  	
    var aligned = squel.select().
      field(idTemplate(metadata), "id");

      if (metadata.partition) {
      	aligned.field(partitionTemplate(metadata), "day");
      }

      aligned.
      	field("MD5(lhs." + versionColumns.join(" || lhs.") + ")", "version"). // Very hacky join :-(
      	field("CAST(ceil((CAST(COUNT(*) AS decimal) / 100)) AS UNSIGNED)", "bucket").
      	from(metadata.table, "lhs").
      	join(metadata.table, "rhs", joinTemplate(metadata));

      if (metadata.partition) {
      	aligned.
      		group("day").
      		group("lhs.id").
      		group("lhs.version").
      		order("day", true);
      } else {
      	aligned.
      		group("lhs.id").
      		group("lhs.version");
	    }

      aligned.order("bucket", true);

    var bucketed = squel.select().
    	field("bucket");

    	if (metadata.partition) {
    		bucketed.field("day");	
    	}
                  
      bucketed.      	
      	field("LOWER(HEX(MD5(GROUP_CONCAT(version,''))))", "digest").
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
	      field("LOWER(HEX(MD5(GROUP_CONCAT(digest,''))))", "digest").
	      from(bucketed, "bucketed").
	      group("day").
	      order("day", true);

	    var monthly = squel.select().
	      field("STRFTIME('%Y-%m-01', day)", "month").
	      field("LOWER(HEX(MD5(GROUP_CONCAT(digest,''))))", "digest").
	      from(daily, "daily").
	      group("month").
	      order("month", true);

	    var yearly = squel.select().
	      field("STRFTIME('%Y-01-01', month)", "year").
	      field("LOWER(HEX(MD5(GROUP_CONCAT(digest,''))))", "digest").
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