var mongo = require('mongojs');

module.exports = function (config) {
	var collections = config.collections.map(function (c) {
		return c.name;
	});

	return mongo(config.mongo.connection, collections);
};
