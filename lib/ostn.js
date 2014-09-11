var request = require('request');

var Ostn = function() {
	var self = this;
	self.newRepo = function(repo, callback) {
		request.post({
			url: 'http://localhost:3000/repos',
			form: { name: repo }
		}, function (err, response, body) {
			if (err) {
		  		callback(err);
		  	} else {
		  		callback(null, body);	
		  	}
		})
	} 
}

module.exports = Ostn;