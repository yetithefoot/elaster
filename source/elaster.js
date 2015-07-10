var _ = require('underscore');
var moment = require('moment');
var async = require('async');
var through = require('through');
var single = require('single-line-log');
var highland = require('highland');
var db, elastic;
var options = {
	simultaneousOperations: 1000
};

require('colors');

function format(duration) {
	return duration.hours() + ':' + duration.minutes() + ':' + duration.seconds() + ':' + duration.milliseconds();
}

function exportCollection(desc, settings, callback) {
	var collection = db[desc.name];
	var query = desc.query || {};

	if (!collection) {
		return callback('collection ' + desc.name + ' does not exist.');
	}

	console.log(('====> exporting collection [' + desc.name + ']').bold.white);

	var started = moment();

	async.waterfall([
		function (next) {
			console.log('----> checking connection to elastic');
			elastic.ping({requestTimeout: 1000}, function (err) {
				next(err);
			});
		},
		function (next) {
			console.log('----> dropping existing index [' + desc.index + ']');
			elastic.indices.delete({index: desc.index}, function (err) {
				var indexMissing = err && err.message.indexOf('IndexMissingException') === 0;
				next(indexMissing ? null : err);
			});
		},
		function (next) {
			console.log('----> creating new index [' + desc.index + ']');
			elastic.indices.create({index: desc.index}, function (err) {
				next(err);
			});
		},
		function (next) {
			console.log('----> close index connection[' + desc.index + ']');
			elastic.indices.close({index: desc.index}, function (err) {

				next(err);
			});
		},
		function (next) {
			console.log('----> initialize index settings');

			if (!settings) {
				return next();
			}
			elastic.indices.putSettings({index: desc.index, body: settings}, function (err, resp) {
				next(err);
			});
		},
		function (next) {
			console.log('----> initialize index mapping');

			if (!desc.mappings) {
				return next();
			}

			elastic.indices.putMapping({index: desc.index, type: desc.type, body: desc.mappings }, function (err) {
				next(err);
			});
		},
		function (next) {
			console.log('----> open index connection[' + desc.index + ']');
			elastic.indices.open({index: desc.index}, function (err) {
				next(err);
			});
		},
		function (next) {
			console.log('----> analizing collection [' + desc.name + ']');
			collection.count(query, function (err, total) {
				if (err) {
					return next(err);
				}

				console.log('----> find ' + total + ' documentents to export');
				next(null, total);
			});
		},
		function (total, next) {
			console.log('----> streaming collection to elastic');

			var takeFields = through(function (items) {
				this.queue(_.map(items, function(item){
					if (desc.fields) {
						item = _.pick(item, desc.fields);
					}
					return item;
				}));
			});

			var postToElastic = through(function (item) {
				var me = this;
				var bulkBody = [];
				me.pause();

				for (i = 0; i < item.length; i++){
					bulkBody.push({
						index: {
							_index: desc.index,
							_type: desc.type,
							_id: item[i]._id.toString()
						}
					});
					delete item[i]._id;
					bulkBody.push(item[i]);
				}
				//console.log(item.length);
				//console.dir(bulkBody);
				// me.queue(item.length);
				// me.resume();

				elastic.bulk({
					body: bulkBody
				}, function (err, resp) {
					if (err) {
						//console.error(('response timeout or failed to connect.').bold.red);
						console.dir(err);
						return next(err);
					}
					if (resp.errors){
						console.error(('failed to create a document in elastic.').bold.red);
						return next(resp);
					}

					me.queue(item.length);
					me.resume();
				});
			});

			var progress = function () {
				var count = 0;
				return through(function (length) {
					count += length;
					var percentage = Math.floor(100 * count / total);
					single(('------> processed ' + count + ' documents [' + percentage + '%]').magenta);
				});
			};

			var cursor = collection.find(query).sort({_id: 1});

			var stream = highland(cursor)
				.batch(options.simultaneousOperations)
				.pipe(takeFields)
				.pipe(postToElastic)
				.pipe(progress());

			stream.on('end', function (err) {
				next(err, total);
			});
		},
	], function (err) {
		if (err) {
			console.error(('====> collection [' + desc.name + '] - failed to export.\n').bold.red);
			console.error(err);
			return callback(err);
		}

		var duration = moment.duration(moment().diff(started));

		console.log(('====> collection [' + desc.name + '] - exported successfully.').green);
		console.log(('====> time elapsed ' + format(duration) + '\n').green);

		callback(null);
	});
}

function close() {
	async.each([db, elastic], _close);

	function _close(conn, callback) {
		conn.close(callback);
	}
}

function exporter(config) {
	db = require('./db')(config);
	elastic = require('./elastic')(config);
	
	var collections = config.collections;
	var exports = collections.map(function (c) {
		return function (callback) {
			exportCollection(c, config.settings, callback);
		};
	});

	async.series(exports, close);
}

module.exports = {
	run: exporter
};