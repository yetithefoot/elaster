require('colors');
const moment = require('moment');
const async = require('async');
const through = require('through');
const single = require('single-line-log');
const highland = require('highland');
const elasticsearch = require('elasticsearch');
const mongojs = require('mongojs');
const defaults = require('lodash.defaults');
const pick = require('lodash.pick');
let db, elastic;


function format(duration) {
	return duration.hours() + ':' + duration.minutes() + ':' + duration.seconds() + ':' + duration.milliseconds();
}

function exportCollection(desc, settings, options = { }, callback) {

	options = defaults(options, { batchSize: 1000 });

	const collection = db[desc.name];
	const query = desc.query || {};

	if (!collection) {
		return callback('collection ' + desc.name + ' does not exist.');
	}

	console.log(('====> exporting collection [' + desc.name + ']').bold.white);

	var started = moment();

	async.waterfall([
		next => { 
			console.log('----> checking connection to elastic');
			elastic.ping(
				{ requestTimeout: 1000 },
				err => {
					next(err);
				}
			);
		},
		next => {
			console.log('----> dropping existing index [' + desc.index + ']');
			elastic.indices.delete(
				{ index: desc.index },
				err => {
					var indexMissing = err && err.message.indexOf('IndexMissingException') === 0;
					next(indexMissing ? null : err);
				}
			);
		},
		next => {
			console.log('----> creating new index [' + desc.index + ']');
			elastic.indices.create(
				{ index: desc.index },
				err => {
					next(err);
				}
			);
		},
		next => {
			setTimeout(next.bind(this, null), 1000);
		},
		next => {
			console.log('----> close index connection[' + desc.index + ']');
			elastic.indices.close(
				{ index: desc.index },
				err => {
					next(err);
				}
			);
		},
		next => {
			console.log('----> initialize index settings');

			if (!settings) {
				return next();
			}
			elastic.indices.putSettings(
				{index: desc.index, body: settings},
				(err, resp) => {
					next(err);
				}
			);
		},
		next => {
			console.log('----> initialize index mapping');

			if (!desc.mappings) {
				return next();
			}

			elastic.indices.putMapping(
				{index: desc.index, type: desc.type, body: desc.mappings },
				err => {
					next(err);
				}
			);
		},
		next => {
			console.log('----> open index connection[' + desc.index + ']');
			elastic.indices.open(
				{index: desc.index},
				err => next(err)
			);
		},
		next => {
			console.log('----> analizing collection [' + desc.name + ']');
			collection.count(query,
				(err, total) => {
					if (err) {
						return next(err);
					}

					console.log('----> find ' + total + ' documentents to export');
					next(null, total);
				}
			);
		},
		(total, next) => {
			console.log('----> streaming collection to elastic');
			if(desc.preformers) {
				console.log(`-----> preformers run for fields: ${Object.keys(desc.preformers)}`);
			}
			const takeFields = through(function(items) {

				this.queue(items.map(item => {
					if (desc.fields) {
						item = pick(item, desc.fields);
					}

					// if there is preformers (pre index transformer) for field run it
					// cases: sometimes before indexing we need to transform initial value somehow
					if(desc.preformers) {
						for(let field in desc.preformers){
							if(item[field]){
								const fn = desc.preformers[field];
								item[field] = fn(item[field]);
							}
						}
					}
					return item;
				}));
			});

			const postToElastic = through(function (item) {

				const bulkBody = [];
				this.pause();

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

				elastic.bulk({
					body: bulkBody
				}, (err, resp) => {
					if (err) {
						//console.error(('response timeout or failed to connect.').bold.red);
						console.error(('failed to create a document in elastic. err').bold.red, JSON.stringify(err));
						//return next(err);
						// this.resume();
					}
					if (resp.errors){
						try {
						const errors = resp.items.filter(item => (item.index.status >= 300 || item.index.status < 200) || item.index.error);
						console.error(('failed to create a document in elastic. resp').bold.red, JSON.stringify(errors));
					}catch(err){}
						//return next(resp);
						// this.resume();
					}

					this.queue(item.length);
					this.resume();
				});
			});

			const progress = function () {
				let count = 0;
				return through(function (length) {
					count += length;
					const percentage = Math.floor(100 * count / total);
					single(('------> processed ' + count + ' documents [' + percentage + '%]').magenta);
				});
			};

			const cursor = collection.find(query).sort({_id: 1});

			const stream = highland(cursor)
				.batch(options.batchSize || 1000)
				.pipe(takeFields)
				.pipe(postToElastic)
				.pipe(progress());

			stream.on('end', function (err) {
				next(err, total);
			});
		},
		(resp, next) => {
			if(!options.setAlias) return next();
			console.log(`----> setting alias "${options.setAlias}" to [${desc.index}]`);
			elastic.indices.deleteAlias({
				index:'*',
				name: options.setAlias
			},function (err) {
					if (err) {
						// ignore error
						// console.error(('failed to delete existing alias.').bold.red);
						// console.dir(err);
						// return next(err);
					}
					elastic.indices.putAlias({
						index: desc.index,
						name: options.setAlias
					},function (err) {
						if (err) {
							console.error(('failed to set alias.').bold.red);
							console.dir(err);
							return next(err);
						}
						next();
					});
				});
		},
	], function (err) {
		if (err) {
			console.error(('====> collection [' + desc.name + '] - failed to export.\n').bold.red);
			console.error(JSON.stringify(err,null,2));
			return callback(err);
		}

		const duration = moment.duration(moment().diff(started));

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

function run(config) {
	db = mongojs(config.mongo.connection);
	elastic = elasticsearch.Client(config.elastic);

	// export all collections
	const collections = config.collections;
	const exports = collections.map(c => callback => exportCollection(c, config.settings, config.options, callback));
	async.series(exports, close);
}

module.exports = {
	run
};
