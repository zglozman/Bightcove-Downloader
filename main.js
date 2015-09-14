var winston = require('winston');
require('newrelic');
//
// Requiring `winston-papertrail` will expose
// `winston.transports.Papertrail`



var express = require('express');
var http = require('http');
var path = require('path');
var app = express();
var sprintf = require('sprintf').sprintf;
var sscanf = require('scanf').sscanf;
var AWS = require('aws-sdk');
AWS.config.update({ accessKeyId: 'YOURKEYS', secretAccessKey: 'YOURKEYS' });
var s3 = new AWS.S3();
var urlParser = require('url');
var testVideoId = '1766429771001';



var knox = require('knox');
//AWS.config.loadFromPath('./aws-config.json');


AWS.config.region = 'us-east-1';




var testbcd = '1800831757001';

var __dirname;
/**
 * Don't hard-code your credentials!
 * Export the following environment variables instead:
 *
 * export AWS_ACCESS_KEY_ID='AKID'
 * export AWS_SECRET_ACCESS_KEY='SECRET'
 */



// all environments
app.set('port', process.env.PORT || 3031);
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.json());
app.use(express.urlencoded());
app.use(express.methodOverride());
app.use(app.router);
app.use(require('stylus').middleware(path.join(__dirname, 'public')));
app.use(express.static(path.join(__dirname, 'public')));
port = app.get('port');


var queue = require("queue-async");

var brightcoveQueryStrings = {
    host: "http://api.brightcove.com/",

    TagQuery:'/services/library?command=search_videos&token=&any=tag%s&get_item_count=true&media_delivery=http&page_number=%s',
    IdQuery: '/services/library?command=find_video_by_id&video_id=%s&media_delivery=http&token=',
    createTagQuery: function (userId, pageNumber, bcId) {
        if( bcId == undefined || bcId.length == 0) {
            return sprintf(this.TagQuery,  encodeURIComponent(':##@' + userId + '##@'), pageNumber);
        }
        else {
            return sprintf(this.IdQuery, bcId,  pageNumber);
        }

    },
    createBidQuery: function (bcd) { return sprintf(this.bidQuery, bcd) },
};

function downloadFromBcAndUploadToS3(url, outputPath, jobId, _options, cb) {
   // console.log("beggining download of " + outputPath + " " +  jobId)


    var params_for_head_request = {
        Bucket: 'lfe-user-videos', /* required */
        Key: outputPath,
    };

    function downloadFileFromBc(url, _options, cb) {
        http.get(url, function (res) {

            var params = {
                Bucket: 'lfe-user-videos', /* required */
                Key: outputPath,
                ACL: 'public-read',
                Body: res,
                ContentType: res.headers['content-type'],
                ContentLength: res.headers['content-length'],
            };

            var firstRequestInFile = true;

            res.on("socket", function (socket) {
                console.log("onsocket");
            });
            res.on("connect", function (socket) {
                console.log("onsocket");
            });
            res.on("connection", function (socket) {
                console.log("onsocket");
            });
            res.on("error", function (socket) {
                console.error(" data error error must retry");
            });


            s3.upload(params, function (data, err) {
                if (err) console.log(err, err.stack); // an error occurred
                else {
                }
                cb(data, _options);
            });
        });
    }

    function preformHeadRequest(url, callback){
        var parsedUrl =  urlParser.parse(url);
        var optionsHttp = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || 80,
            path: parsedUrl.pathname,
            method: 'HEAD'
        };
        http.get(optionsHttp, function (response) {

            callback(response.headers)
        });
    }

    s3.headObject(params_for_head_request, function(err, data) {
        if(err == null){
            if(_options.size) {
                if (data.ContentLength == _options.size) {
                    cb();
                    return;
                } else {
                    console.log("file  found in s3 but size missmatch redownloading " + url);
                    downloadFileFromBc(url, _options, cb);
                }
            }else{
                preformHeadRequest(url, function(headers) {
                    var contentLength = headers['content-length'];

                    if(contentLength  != data.ContentLength){
                        console.log("content length is different re-downloading " + url);
                        downloadFileFromBc(url, _options, cb);

                    }else{
                        cb();
                    }
                });
            }
        }else{
            console.log("file  not found in s3 downloading " + url);
            downloadFileFromBc(url, _options, cb);

        }
    });





}

initHttpServer();
//testPostMethod();


app.get('/',  function (req, res) {
    res.send("Ok");

});

app.get('/toggle/:id',  testToggle);


function executeMediaApiQuery(userId, pageNumber, bcId, callback) {

    var a = {
        host: 'api.brightcove.com',
        path: brightcoveQueryStrings.createTagQuery(userId, pageNumber, bcId)
    };
    var err = "";

    var combniedItemList = [];
    http.get(a, function (response) {
        // Continuously update stream with data
        var body = '';
        response.on('data', function (d) {
            body += d;
        });
        response.on('error', function (d) {
            err = d;
        });

        response.on('end', function () {
            var parsedBody = JSON.parse(body);
            callback(parsedBody, pageNumber, err);
        });
    });
}


function queryBrightCoveForUserVideos(userId, bcId, finalCallback) {
    var pageNumber = 0;
    var combinedArray = [];
    var err = "";
    var combinedJSON = {};
     function executePagedQuery(jsonResult, pageNumber, bcId, err1) {
        err += err1;

        combinedArray = combinedArray.concat(jsonResult.items);

        if(jsonResult.total_count == 0){
            finalCallback(combinedArray, err);
            return;
        }
        if (combinedArray.length < (jsonResult.total_count || Infinity)) {
            pageNumber++;
            executeMediaApiQuery(userId, pageNumber,bcId, executePagedQuery,  err);
        }
        else {
            finalCallback(combinedArray, err);
        }
    }
    if(bcId.length ==0)
        return executePagedQuery(combinedArray, -1);
    else{
        executeMediaApiQuery(userId, pageNumber,bcId,  function(jsonResult){
            finalCallback(['huj', jsonResult], err);

        });

    }
}
var q = queue(15);


function uploadAJob(req, _item, i, launchedJobIndex, renditionReporter, callback) {
    var amazonPath = req.body['userId'] + "/" + _item.id + "/" + _item.renditions[i].id + "/" + _item.renditions[i].id + "." + _item.renditions[i].videoContainer;
    var location = req.body['userId'] + "/" + _item.id + "/";


    var url = _item.renditions[i].url;
    //console.log('Launching job #' + launchedJobIndex + " " + amazonPath);
    //function downloadFromBcAndUploadToS3(url, outputPath, jobId, metaData, cb)

    var options = {
        rendition: _item.renditions[i],
        amazonPath: amazonPath,
        item: _item,
        location: location,
        size: _item.renditions[i].size

    };

    downloadFromBcAndUploadToS3(_item.renditions[i].url, amazonPath, launchedJobIndex, options, function (data, option, err) {
        if(option != undefined && option != null) {
            option.rendition.BcId = option.item.id;
            option.rendition.location = option.location;
            option.rendition.S3Url = "https://s3.amazonaws.com/lfe-user-videos/" + option.amazonPath;
            option.rendition.CloudFrontPath = "http://uservideos.lfe.com/" + option.amazonPath
            option.rendition.numberOfRenditions = option.item.renditions.length;
            renditionReporter(option.rendition);
        }
        callback();
    });
}
function processQueuedJobs(videosToDownload, req, res,renditionReporter) {

    var queuedJobIndex = 0;
    var launchedJobIndex= 0;

    for(index=1; index<videosToDownload.length; index++){
        item = videosToDownload[index];
        q.defer(function(cb) {
            downloadFromBcAndUploadToS3(item.videoStillURL, req.body['userId'] + "/" + item.id + "/still.jpg", 0, { },  function (err) {
                cb();
            });
        });

        q.defer(function (cb) {
            downloadFromBcAndUploadToS3(item.thumbnailURL, req.body['userId'] + "/" + item.id + "/thumbnail.jpg", 0, {},  function (ee) {
                cb();
            });
        });

        for (i = 0; i < item.renditions.length; i++) {
            var rendition = item.renditions[i];
            var re = /(?:\.([^.]+))?$/;

            queuedJobIndex++;
            q.defer(uploadAJob, req,  item, i, launchedJobIndex, renditionReporter );
                //function (_item, i, launchedJobIndex, callback, cb) {

                //}
            launchedJobIndex++;
        }
    }
    console.log("queuing jobs " + queuedJobIndex );
    res.status(202).send('All qeued');
}

app.post('/downloadUserVideos', function (req, res) {

    var qStarted = false;
    var video;
    var uId = req.body['userId'];
    var host = req.body['host'];
    var path = req.body['path'];
    var port = req.body['port'];
    var bcId = req.body['bcId'];
    if(bcId == undefined)
        bcId="";

    console.log("Got a request Uid " + uId + " HOST" + host +" path " + path + " port" + port + " bcid " + bcId);
    //console.log(req.headers);
   // console.log(req.body);
        // final callback
    queryBrightCoveForUserVideos(uId, bcId, function (combinedArray, err, combinedJSON){

        processQueuedJobs(combinedArray, req, res, function renditionReporter(renditionResponse){

            var jsonPostData = JSON.stringify(renditionResponse);
            var options = {
                hostname: host,
                port: port,
                path: path,
                method: 'POST',
                             headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': jsonPostData.length,
                }
            };
            try {
                var req = http.request(options);
            }catch(e){
                console.log('connection exception : ' + e);
                req.end();
                return;

            }
            //console.log("sendin response " + jsonPostData);
            req.on('error', function (e) {
                console.log('problem with request: ' + e.message);
            });

            req.write(jsonPostData);
            req.end();
        });
       // console.dir(cominedArray);
    })

    q.awaitAll(function (error, results) {
        console.log("all done!");
    });
 });


function initHttpServer() {
    console.log('Starting http server:', app.get('port'));
    http.createServer(app).listen(app.get('port'), function (error, result) {
        console.log('http.createServer(app) callback', arguments);
        console.log('Express server listening on port ' + app.get('port'));
    });
}

function testToggle (req, res) {
    var userId = req.params.id;
    var bcid = req.query.bcid;

    runTransferJobs(userId, bcid );

    console.log('Toggle GET started jobs with userId: ' + userId);

    res.send("Ok");
}
function runTransferJobs (id, bcid) {
    var postData = {
        userId: id,
        bcId: bcid,
        host: '',
        path: '',

    };

    //var postData = {
    //    UserId: '422',
    //    callbackHost: '',
    //    callbackPath: ''
    //};
    var jsonPostData = JSON.stringify(postData);
    var options = {
        hostname: 'localhost',
        port: app.get('port'),
        path: '/downloadUserVideos',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': jsonPostData.length,
            'Connection': 'Keep-Alive'
        }
    };

    var req = http.request(options);

    req.on('error', function (e) {
        console.log('problem with request: ' + e.message);
    });

    req.write(jsonPostData);
    req.end();
}
