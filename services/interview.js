var request = require('request');

var Interview = function() {
	var self = this;
	self.nextQuestion = function(callback) {
		request('http://localhost:3000', function (err, response, body) {
			if (err) {
		  		callback(err);
		  	} else {
		  		callback(null, body);	
		  	}
		})
	} 
}

module.exports = Interview;