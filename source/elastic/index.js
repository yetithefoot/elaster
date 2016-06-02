var elasticsearch = require('elasticsearch');

module.exports = function (config) {
	return elasticsearch.Client(config.elastic);
};