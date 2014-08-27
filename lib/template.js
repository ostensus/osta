var squel = require("squel");
var doT   = require('dot');

var idTemplate        = doT.template("lhs.{{= it.id.name }}");
var joinTemplate      = doT.template("lhs.{{= it.id.name }} >= rhs.{{= it.id.name }}");
var partitionTemplate = doT.template("STRFTIME('%Y-%m-%d',lhs.{{= it.partition.name }})");

var TemplateBuilder = function() {
	var self = this;

  self.buildTemplate = function(metadata) {

  	var versionColumns = [metadata.version.name];

    var aligned = squel.select().
      field(idTemplate(metadata), "id").
      field(partitionTemplate(metadata), "day").
      field("MD5(lhs." + versionColumns.join(" || lhs.") + ")", "version"). // Very hacky join :-(
      field("CAST(ceil((CAST(COUNT(*) AS decimal) / 100)) AS UNSIGNED)", "bucket").
      from(metadata.table, "lhs").
      join(metadata.table, "rhs", joinTemplate(metadata)).
      group("day", "id", "version").
      order("day", true).
      order("bucket", true);

    var bucketed = squel.select().
      field("day").
      field("bucket").
      //field("MD5(GROUP_CONCAT(version ORDER BY id ASC separator ''))", "digest").
      field("LOWER(HEX(MD5(GROUP_CONCAT(version,''))))", "digest").
      from(aligned, "aligned").
      group("day", "bucket").
      order("day", true).
      order("bucket", true);

    var daily = squel.select().
      field("day").
      //field("MD5(GROUP_CONCAT(digest ORDER BY bucket ASC separator ''))", "digest").
      field("LOWER(HEX(MD5(GROUP_CONCAT(digest,''))))", "digest").
      from(bucketed, "bucketed").
      group("day").
      order("day", true);

    var monthly = squel.select().
      field("STRFTIME('%Y-%m-01', day)", "month").
      //field("MD5(GROUP_CONCAT(digest ORDER BY day ASC separator ''))", "digest").
      field("LOWER(HEX(MD5(GROUP_CONCAT(digest,''))))", "digest").
      from(daily, "daily").
      group("month").
      order("month", true);

    var yearly = squel.select().
      field("STRFTIME('%Y-01-01', month)", "year").
      //field("MD5(GROUP_CONCAT(digest ORDER BY month ASC separator ''))", "digest").
      field("LOWER(HEX(MD5(GROUP_CONCAT(digest,''))))", "digest").
      from(monthly, "monthly").
      group("year").
      order("year", true);

  	return yearly.toString();
  	//return monthly.toString();
  }
}

module.exports = TemplateBuilder;