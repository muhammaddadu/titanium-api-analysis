/*
 * Download, Parse and Sort Titanium Module API JSON
 * Gary Mathews
 */

// STREAMS
var fs = require('fs');
var MemoryStream = require("memorystream");

// ZLIB
var zlib = require('zlib');

// LINE-CHOMPER
var chomp = require("line-chomper").chomp;

// NODE-S3
var s3 = require("s3");

// GLOBAL VARIABLES
var api_array = [];
var processes = 0;

var limit = 2;

// S3
var bucket = "appcelerator.analytics.datastore"
var client = s3.createClient({
	s3Options: {
		accessKeyId: "",
		secretAccessKey: "",
	},
});

// TASK
processS3("mobile/2014/08/12/");
//processS3("desktop/2014/08/12/");

function processS3(remoteFolder) {
	var params = {
      recursive: true,
      s3Params: {
        Bucket: bucket,
        Prefix: remoteFolder,
      },
    };
    var finder = client.listObjects(params);
    finder.on('data', function(data) {
    	for (var i = 0; i < data.Contents.length; i++) {
      		
      		if (limit > 0 || limit == -1) {
      			limit--;
      			console.log(data.Contents[i].Key);
      			parseS3(data.Contents[i].Key);
      		}

      	}
    });
}

function parseS3(remotePath) {

	var rwstream = new MemoryStream();
	var wstream = new MemoryStream();
	chomp(wstream, processLines);
	rwstream.pipe(zlib.createGunzip()).pipe(wstream);

	var params = {
		writeStream: rwstream,

		s3Params: {
			Bucket: bucket,
			Key: remotePath,
		},
	};

	var downloader = client.downloadFile(params);
}

function parseJSON(file) {
	processes++;
	chomp(file, processLines);
}

function parseGZIP_JSON(file) {
	processes++;
	var rstream = fs.createReadStream(file);
	var wstream = new MemoryStream();
	chomp(wstream, processLines);
	rstream.pipe(zlib.createGunzip()).pipe(wstream);
}

function processLines(err, lines) {

	console.log("Processing JSON...");

    lines.forEach(function (line) {

    	var parse = JSON.parse(line);
    	if (parse.type == "ti.apiusage") {

    		apis = JSON.parse(parse.data).usage;

			for (var key in apis) {

				if (api_array[key] == null) {
					api_array[key] = apis[key];
				} else {
					api_array[key] += apis[key];
				}

			}
    	}

    });

	processes--;
	if (processes == 0) {
    	sort(api_array, function(key, value) {
			console.log(key + "=" + value);
		});
    }
}

function sort(array, callback) {
    var sort_array = [];

    for (var key in array)
    	sort_array.push([key, array[key]]);

    sort_array.sort(function(a,b) {
    	return a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0;
    });
	
    var length = sort_array.length;
    while (length--) callback(sort_array[length][0], sort_array[length][1]);
}
