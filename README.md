# Mongo_to_ES

MongoDB collection to Elastic Search index exporter.

## Usage

Clone the repo,

```
npm install mongo_to_es
```

## How to run
```
var elaster = require('./index.js');
elaster.run({
	mongo: {
		connection: 'mongodb://localhost:27017/picsio_dev'
	},

	elastic: {
		host: {
			host: 'localhost',
			port: 9200
		},

		requestTimeout: 5000
	},

	settings: {
		"analysis": {
			"analyzer": {
				"generic_name_analyzer": {
					"type": "custom",
					"tokenizer": "icu_tokenizer",
					"filter": [
						"word_split",
						"icu_folding",
						"english_stop"
					]
				},
				"trigram_name_analyzer": {
					"type": "custom",
					"tokenizer": "icu_tokenizer",
					"filter": [
						"icu_folding",
						"english_stop",
						"trigram_filter"
					]
				}
			},
			"filter": {
				"word_split": {
					"type": "word_delimiter",
					"preserve_original": 1
				},
				"english_stop": {
					"type": "stop",
					"stopwords": "_english_"
				},
				"trigram_filter": {
					"type": "ngram",
					"min_gram": 3,
					"max_gram": 3
				}
			}
		}
	},

	collections: [{
		name: 'images',
		index: 'images',
		type: 'image',
		mappings: {
			"image": {
				"properties": {
					"name": {
						"type": "multi_field",
						"fields": {
							"name": {
								"type": "string",
								"analyzer": "generic_name_analyzer"
							},
							"trigram": {
								"type": "string",
								"analyzer": "trigram_name_analyzer"
							}
						}
					}
				}
			}
		}
	}]
});



## Licence (MIT)

Copyright (c) 2015, myzlio@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
