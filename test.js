//require('newrelic');
var http = require('http'),
    cluster = require('cluster'),
    os = require('os'),
    url = require('url'),
    crypto = require('crypto'),
    common = require('./utils/common.js'),
    cryptoApi = require('./utils/semusi.crypto.js'),
    cacheApi = require('./utils/semusi.cache.js'),
    apiRouter = require('./utils/api.router.js'),
    botsApi = require('./bots/bots.common.js'),
    kafkaProducer = require('./parts/data/kafka.producer.js'),

    semusiApi = {
        data:{
            usage:require('./parts/data/usage.js'),
            fetch:require('./parts/data/fetch.js'),
            events:require('./parts/data/events.js'),
            backup:require('./parts/data/backup.js'),
            tracking:require('./parts/data/track.js'),
            places:require('./parts/data/places.js'),
            awsHelper:require('./parts/data/aws.data.js'),
            emailTracking: require('./parts/data/emailTracking.js'),
            screenshot: require('./parts/data/screenshot.js')
        },
        mgmt:{
            users:require('./parts/mgmt/users.js'),
            apps:require('./parts/mgmt/apps.js'),
            campaigns:require('./parts/mgmt/campaigns.js'),
            templates:require('./parts/mgmt/templates.js'),
            metrices:require('./parts/mgmt/metrices.js'),
            mail:require('./parts/mgmt/mail.js'),
            netcoreEmail:require('./parts/mgmt/netcore-email.js'),
            smsApi:require('./parts/mgmt/sms.js')
        }
    },
    qs = require('querystring'),
    AWS = require('aws-sdk'),
    fs = require('fs'),
    moment = require("moment"),
    semusiConfig = require('./config');

http.globalAgent.maxSockets = common.config.api.max_sockets || 1024;
//require('events').EventEmitter.prototype._maxListeners = 0;

//This variable will hold the current index of queue to which backup data will be pushed.
var backupQueueIndex = 0;

/*
* Write logs into file
*/
// global.console = {};
// const winston = require('winston');
// const winLogger = new winston.Logger({
//     transports: [
//         new winston.transports.File({
//             level: 'info',
//             filename: common.config.applogs + "/semusi-api-win.log",
//             handleExceptions: true,
//             json: true,
//             //maxsize: 5242880, //5MB
//             //maxFiles: 5,
//             colorize: false
//         })
//     ],
//     exitOnError: false
// });
//
// global.console.log = function(logData){
//     winLogger.info(logData);
// }
/*********** end global log *******************/

function sha1Hash(str, addSalt) {
    var salt = (addSalt) ? new Date().getTime() : "";
    return crypto.createHmac('sha1', salt + "").update(str + "").digest('hex');
}

function md5Hash(str) {
    return crypto.createHash('md5').update(str + "").digest('hex');
}

// Checks app_key from the http request against "apps" collection.
// This is the first step of every write request to API.
function validateAppForWriteAPI(params) {
    common.db.collection('apps').findOne({'key':params.qstring.app_key}, function (err, app) {

        if (!app) {
            if (common.config.api.safe) {
                common.returnMessage(params, 400, 'App does not exist');
            }

            return false;
        }

        params.app_id = app['_id'];
        params.app_cc = app['country'];
        params.appTimezone = app['timezone'];
        params.time = common.initTimeObj(params.appTimezone, params.qstring.timestamp);
        params.callbackurl=app['callbackurl'];

        var updateSessions = {};
        common.fillTimeObject(params, updateSessions, common.dbMap['events']);
        common.db.collection('sessions').update({'_id':params.app_id}, {'$inc':updateSessions}, {'upsert':true}, function(err, res){});

        if (params.qstring.events) {
            semusiApi.data.events.processEvents(params);
        } else if (common.config.api.safe) {
            common.returnMessage(params, 200, 'Success');
        }

        if (params.qstring.begin_session) {
            semusiApi.data.usage.beginUserSession(params);
        } else if (params.qstring.end_session) {
            //semusiApi.data.usage.endUserSession(params);
            if(parseInt(params.qstring.session_duration)>2000){
                console.log("Session duration is too high: "+params.qstring.session_duration);
                console.log(params.req.url);
            }
            if(parseInt(params.qstring.session_duration)<0){
                console.log("Session duration is -ve: "+params.qstring.session_duration);
                console.log(params.req.url);
            }
            if (params.qstring.session_duration) {
                semusiApi.data.usage.processSessionDuration(params, function () {
                    semusiApi.data.usage.endUserSession(params);
                });
            } else {
                semusiApi.data.usage.endUserSession(params);
            }
        } else if (params.qstring.session_duration) {
            semusiApi.data.usage.processSessionDuration(params);
        } else {
            return false;
        }
    });
}

function validateUserForWriteAPI(callback, params) {
    //First try to get this user from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
    cacheApi.getKey(params.qstring.api_key,function(err,result){
        if(err || result==null){
            common.db.collection('members').findOne({'api_key':params.qstring.api_key,isapproved:true}, function (err, member) {
                if(params.qstring.api_key == "cf7c5c13970d915001e8fd485f8a4f4f"){
                    console.log("get from cache time ", moment().valueOf());
                }

                if (!member || err) {
                    common.returnMessage(params, 401, 'User does not exist');
                    return false;
                }
                //Write this result back to Redis so that in frequent requests it is served from redis itself.
                //console.log("Member served from Mongo");
                cacheApi.setKey(params.qstring.api_key,JSON.stringify(member));
                params.member = member;
                callback(params);
            });
        }
        else{
            //console.log("Member served from Redis");
            params.member = JSON.parse(result);
            params.member._id = common.db.ObjectID(params.member._id);
            callback(params);
        }
    });
}


function validateUserForDataReadAPI(params, callback, callbackParam) {
    //First try to get this user from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
    cacheApi.getKey(params.qstring.api_key,function(err,result){
        if(err || result==null){
            common.db.collection('members').findOne({'api_key':params.qstring.api_key}, function (err, member) {
                if (!member || err) {
                    common.returnMessage(params, 401, 'User does not exist');
                    return false;
                }
                else{
                    //Write this result back to Redis so that in frequent requests it is served from redis itself.
                    //console.log("Member served from Mongo");
                    cacheApi.setKey(params.qstring.api_key,JSON.stringify(member));
                    validateApp(params,callback,callbackParam);
                }
            });
        }
        else{
            //console.log("Member served from Redis");
            var member = JSON.parse(result);
            if (!((member.user_of && member.user_of.indexOf(params.qstring.app_id) != -1) || member.global_admin)) {
                common.returnMessage(params, 401, 'User does not have view right for this application');
                return false;
            }
            validateApp(params,callback,callbackParam);

        }
    });
}

function validateApp(params,callback,callbackParam){
    //First try to get this app from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
    cacheApi.getKey(params.qstring.app_id,function(err,result){
        if(err || result==null){
            common.db.collection('apps').findOne({'_id':common.db.ObjectID(params.qstring.app_id + "")}, function (err, app) {
                if (!app) {
                    common.returnMessage(params, 401, 'App does not exist');
                    return false;
                }
                //Write this app back to cache
                cacheApi.setKey(params.qstring.app_id,JSON.stringify(app));
                params.app_id = app['_id'];
                params.appTimezone = app['timezone'];
                params.time = common.initTimeObj(params.appTimezone, params.qstring.timestamp);
                params.defaultuninstallcampaign = app['defaultuninstallcampaign'];
                if(app.createdOn){
                    params.appCreatedOn = app.createdOn;
                }
                if (callbackParam) {
                    callback(callbackParam, params);
                } else {
                    callback(params);
                }
            });
        }
        else{
            var app = JSON.parse(result);
            params.app_id =common.db.ObjectID(app['_id']);
            params.appTimezone = app['timezone'];
            params.time = common.initTimeObj(params.appTimezone, params.qstring.timestamp);
            params.defaultuninstallcampaign = app['defaultuninstallcampaign'];
            if(app.createdOn){
                params.appCreatedOn = app.createdOn;
            }
            if (callbackParam) {
                callback(callbackParam, params);
            } else {
                callback(params);
            }
        }

    });
}

function validateUserForAppStats(params,callback){
    common.db.collection('members').findOne({'api_key':params.qstring.api_key}, function (err, member) {
        if (!member || err) {
            common.returnMessage(params, 401, 'User does not exist');
            return false;
        }
        else{
            params.app_ids=[];
            if(params.qstring.app_id){
                params.app_ids.push(params.qstring.app_id);
                callback(params);
            }
            else{
                if(member.global_admin){
                    common.db.collection('apps').find({"isAppDeleted":{ '$ne': 'true' }}).toArray(function (err, apps) {
                        if(apps){
                            apps.forEach(function(app){
                                params.app_ids.push(app._id);
                            });
                            callback(params);
                        }
                    });
                }
                else{
                    member.admin_of.forEach(function(id){
                        params.app_ids.push(id);
                    });
                    callback(params);
                }
            }
        }


    });
}

function validateUserForMgmtReadAPI(callback, params) {
    //First try to get this user from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
    cacheApi.getKey(params.qstring.api_key,function(err,result){
        if(err || result==null){
            common.db.collection('members').findOne({'api_key':params.qstring.api_key,isapproved:true}, function (err, member) {
                if (!member || err) {
                    common.returnMessage(params, 401, 'User does not exist');
                    return false;
                }
                //Write this result back to Redis so that in frequent requests it is served from redis itself.
                //console.log("Member served from Mongo");
                cacheApi.setKey(params.qstring.api_key,JSON.stringify(member));
                params.member = member;
                callback(params);
            });
        }
        else{
            //console.log("Member served from Redis");
            params.member = JSON.parse(result);
            params.member._id = common.db.ObjectID(params.member._id);
            callback(params);
        }
    });
}

function validateUserForLogin(callback, params)
{
    var password = sha1Hash(params.qstring.password);
    var username = params.qstring.username;
    var regexusername = new RegExp('^'+username,'i');

    common.db.collection('members').findOne({"username":regexusername, "password":password},
     function (err, member) {
               if (!member || err) {
                    common.returnMessage(params, 401, 'User does not exist');
                    return false;
                }

                params.member = member;
                common.returnOutput(params, member);
                return true;
            });
}

//This function processes backup post data.
function processPost(request, response, params, callback) {
    var body = "";
    request.on('data', function(data) {
        body += data;
        if(body.length > 256000) {
            //console.log("WARN: Backup data chunk is exceeding message limit.");
            body = "";
            request.connection.destroy();
            common.returnMessage(params, 413, 'Backup data chunk is exceeding message limit.');
            return false;
        }
    });

    request.on('end', function() {

        var postdata = qs.parse(body);
        if(!postdata.api_key){
            common.returnMessage(params, 400, 'Missing parameter "api_key"');
            return false;
        }
        if(!postdata.app_key){
            common.returnMessage(params, 400, 'Missing parameter "app_key"');
            return false;
        }
        if(!postdata.app_id){
            common.returnMessage(params, 400, 'Missing parameter "app_id"');
            return false;
        }
        //validating user for write api
        //First try to get this user from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
        cacheApi.getKey(postdata.api_key,function(err,result){
            if(err || result==null){
                common.db.collection('members').findOne({'api_key':postdata.api_key,isapproved:true}, function (err, member) {
                    if (!member || err) {
                        common.returnMessage(params, 401, 'User does not exist');
                        return false;
                    }
                    else{
                        //Write this result back to Redis so that in frequent requests it is served from redis itself.
                        //console.log("Member served from Mongo");
                        cacheApi.setKey(postdata.api_key,JSON.stringify(member));
                        params.member = member;
                        callback(postdata);
                    }
                });
            }
            else{
                //console.log("Member served from Redis");
                params.member = JSON.parse(result);
                params.member._id = common.db.ObjectID(params.member._id);
                callback(postdata);
            }
        });
    });
}

//This is method handles the encrypted backup data.This function processes backup post data.
function processPostV1(request, response, params, callback) {
    var body = "";
    request.on('data', function(data) {
        body += data;
        if(body.length > 256000) {
            console.log("WARN: Backup data chunk is exceeding message limit.");
            body = "";
            request.connection.destroy();
            common.returnMessage(params, 413, 'Backup data chunk is exceeding message limit.');
            return false;
        }
    });

    request.on('end', function() {
        var postdata = qs.parse(cryptoApi.decryptV1(params.res,body));
        if(!postdata.api_key){
            common.returnMessage(params, 400, 'Missing parameter "api_key"');
            return false;
        }
        if(!postdata.app_key){
            common.returnMessage(params, 400, 'Missing parameter "app_key"');
            return false;
        }
        if(!postdata.app_id){
            common.returnMessage(params, 400, 'Missing parameter "app_id"');
            return false;
        }
        //validating user for write api
        //First try to get this user from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
        cacheApi.getKey(postdata.api_key,function(err,result){
            if(err || result==null){
                common.db.collection('members').findOne({'api_key':postdata.api_key,isapproved:true}, function (err, member) {
                    if (!member || err) {
                        common.returnMessage(params, 401, 'User does not exist');
                        return false;
                    }
                    else{
                        //Write this result back to Redis so that in frequent requests it is served from redis itself.
                        //console.log("Member served from Mongo");
                        cacheApi.setKey(postdata.api_key,JSON.stringify(member));
                        params.member = member;
                        callback(postdata);
                    }
                });
            }
            else{
                //console.log("Member served from Redis");
                params.member = JSON.parse(result);
                params.member._id = common.db.ObjectID(params.member._id);
                callback(postdata);
            }
        });
    });
}

//This is method handles the encrypted backup data.This function processes backup post data.
function processPostV2(request, response, params, callback) {
    var body = "";
    request.on('data', function(data) {
        body += data;
        if(body.length > 256000) {
            console.log("WARN: Backup data chunk is exceeding message limit.");
            body = "";
            request.connection.destroy();
            common.returnMessage(params, 413, 'Backup data chunk is exceeding message limit.');
            return false;
        }
    });

    request.on('end', function() {
        var postdata = qs.parse(cryptoApi.decryptV2(params.res,body));
        if(!postdata.api_key){
            common.returnMessage(params, 400, 'Missing parameter "api_key"');
            return false;
        }
        if(!postdata.app_key){
            common.returnMessage(params, 400, 'Missing parameter "app_key"');
            return false;
        }
        if(!postdata.app_id){
            common.returnMessage(params, 400, 'Missing parameter "app_id"');
            return false;
        }
        //validating user for write api
        //First try to get this user from Cache(Redis), if does not exist then hit mongo. and store back to Cache.
        cacheApi.getKey(postdata.api_key,function(err,result){
            if(err || result==null){
                common.db.collection('members').findOne({'api_key':postdata.api_key,isapproved:true}, function (err, member) {
                    if (!member || err) {
                        common.returnMessage(params, 401, 'User does not exist');
                        return false;
                    }
                    else{
                        //Write this result back to Redis so that in frequent requests it is served from redis itself.
                        //console.log("Member served from Mongo");
                        cacheApi.setKey(postdata.api_key,JSON.stringify(member));
                        params.member = member;
                        callback(postdata);
                    }
                });
            }
            else{
                //console.log("Member served from Redis");
                params.member = JSON.parse(result);
                params.member._id = common.db.ObjectID(params.member._id);
                callback(postdata);
            }
        });
    });
}

//This function sends backupdata chunk to Queue
function sendBackupDataToQueue(data,params){
    var sqs = new AWS.SQS();
    var boxParams = {
        MessageBody: JSON.stringify(data),
        QueueUrl: semusiConfig.DataBackupQueueList[backupQueueIndex],
        DelaySeconds: 0
    };
    sqs.sendMessage(boxParams, function (err, data) {
        if (err) {
            console.log(err);
            mail.sendAlertNotification("Alert: SQS failure.","Could not write backup data to sqs");
            try{
               fs.writeFile(semusiConfig.SQSFailureLog+"/"+common.getCurrentEpochTime(), data, function(err) {
                    if(err) {
                        return console.log(err);
                    }
                });
            }
            catch(e){
                console.log('SQSFailureLog catch error:'+JSON.stringify(e));
            }
            //common.returnMessage(params, 200, 'false');
            //return true;
        }
        else {
            //common.returnMessage(params, 200, 'true');
            //return true;
        }
    });

    //Change the queueindex so that next itme gets inserted to the next queue.
    if(backupQueueIndex == semusiConfig.BackupQueueLength-1){
        backupQueueIndex = 0;
    }
    else{
        backupQueueIndex++;
    }
}

//This function will write the callback data to a log file.
function createCallBackLog(data,channel){
    var logDir = semusiConfig.callbackLog;
    var fileName = logDir + "/"+ channel + "_" + moment().year()+''+(moment().month()+1)+''+moment().date()+".log";
    var log = fs.createWriteStream(fileName, {'flags': 'a'});
    // use {'flags': 'a'} to append and {'flags': 'w'} to erase and write a new file
    log.end(JSON.stringify(data,null,4)+",");
};
function validateUserForSignUp(callback, params) {
    callback(params);
}

if (cluster.isMaster) {

    var workerCount = (common.config.api.workers)? common.config.api.workers : os.cpus().length;

    for (var i = 0; i < workerCount; i++) {
        cluster.fork();
    }

    var maxWorkerCrashes = 4;
    cluster.on('exit', function(worker) {
        cluster.fork();
    });

} else {

    http.Server(function (req, res) {
        res.setHeader("Access-Control-Allow-Origin", "*");
        //Check if url is android encrypted version.
        if(url.parse(req.url, true).pathname.indexOf("V2")>0){
            var urlparts = req.url.split('?');
            if(urlparts.length>1){
                var decryptUrl = cryptoApi.decryptV1(res, urlparts[1]);
                if(decryptUrl){
                    //Remove unwanted unicode characters appended while decryption.
                    decryptUrl = decryptUrl.replace(/[^A-Za-z 0-9 \.,\?""!@#\$%\^&\*\(\)-_=\+;:<>\/\\\|\}\{\[\]`~]*/g, '') ;
                    req.url = urlparts[0] +"?"+ decryptUrl;
                }
                else{
                    return false;
                }

            }
            else{
                return false;
            }

        }
        else if(url.parse(req.url, true).pathname.indexOf("V3")>0){ //IOS encryption version.
            var urlparts = req.url.split('?');
            if(urlparts.length>1){
                var decryptUrl = cryptoApi.decryptV2(res, urlparts[1]);
                if(decryptUrl){
                    //Remove unwanted unicode characters appended while decryption.
                    decryptUrl = decryptUrl.replace(/[^A-Za-z 0-9 \.,\?""!@#\$%\^&\*\(\)-_=\+;:<>\/\\\|\}\{\[\]`~]*/g, '') ;
                    req.url = urlparts[0] +"?"+ decryptUrl;
                }
                else{
                    return false;
                }

            }
            else{
                return false;
            }
        }

        // convert post request into get
        if(req.method == 'POST') {
            var body = "";
            var params = {
                'res':res,
                'req':req,
                'url': url.parse(req.url, true).pathname
            }

            console.log("params url "+ params.url );

            req.on('data', function(data) {
                body += data;
                if(body.length > 256000) {
                    console.log("WARN: POST API data chunk is exceeding message limit.");
                    body = "";
                    req.connection.destroy();
                    common.returnMessage(params, 413, 'POST API chunk is exceeding message limit.');
                    return false;
                }
            });

            req.on('end', function() {
                var postdata = (typeof body === "string")? JSON.parse(body) : body;
                // encoded metrics that comes in initialize API call
                if(postdata.metrics){
                    postdata.metrics = JSON.stringify(postdata.metrics);
                }

                // encoded events that coems in events API call
                if(postdata.events){
                    postdata.events = JSON.stringify(postdata.events);
                }

                // encoded appData that coems in putAppAndUserData API call
                if(postdata.appData){
                    postdata.appData = JSON.stringify(postdata.appData);
                }

                // encoded deviceData that coems in putAppAndUserData API call
                if(postdata.deviceData){
                    postdata.deviceData = JSON.stringify(postdata.deviceData);
                }

                console.log("method postdata ====>  "+ JSON.stringify(postdata));
                if(!postdata.api_key){
                    common.returnMessage(params, 400, 'Missing parameter "api_key"');
                    return false;
                }
                if(!postdata.app_key){
                    common.returnMessage(params, 400, 'Missing parameter "app_key"');
                    return false;
                }
                if(!postdata.app_id){
                    common.returnMessage(params, 400, 'Missing parameter "app_id"');
                    return false;
                }
                httpRequestListener(req, res, postdata);
            });
        }
        else if(req.method == 'GET') {
            var urlParts = url.parse(req.url, true), queryString = urlParts.query;
            httpRequestListener(req, res, queryString);
        }
    }).listen(common.config.api.port, common.config.api.host || '');
}

// request listener
var httpRequestListener = function(req, res, queryString){

  var urlParts = url.parse(req.url, true),
      paths = urlParts.pathname.split("/"),
      apiPath = "",
      params = {
          'qstring':queryString,
          'res':res,
          'req':req,
          'url': urlParts.pathname
      },postData = {};

        AWS.config.region = semusiConfig.AWS_REGION;
        AWS.config.update({accessKeyId:semusiConfig.AWS_ACCESS_KEY_ID,secretAccessKey:semusiConfig.AWS_SECRET_KEY});

        //If the API route contains newer versions then handle them via API router.
        //V1 API's to send data to sqs/kafka
        //V2 for android to send encrypted data to sqs/kafka
        //V3 for ios to send encrypted data to sqs/kafka
        if(url.parse(req.url, true).pathname.indexOf("/V1/")>0 || url.parse(req.url, true).pathname.indexOf("/V2/")>0 || url.parse(req.url, true).pathname.indexOf("/V3/")>0){
            apiRouter.handleRoute(urlParts.pathname,params,kafkaProducer);
        }
        else if(url.parse(req.url, true).pathname.indexOf("/bots/1.0")>0){
            botsApi.processMessage(params);
        }
        else if(url.parse(req.url, true).pathname.indexOf("/postback")>0){
            postback.processMessage(req, params);
        }
        else{
            if (queryString.app_id && queryString.app_id.length != 24) {
                common.returnMessage(params, 400, 'Invalid parameter "app_id"');
                return false;
            }

            if (queryString.user_id && queryString.user_id.length != 24) {
                common.returnMessage(params, 400, 'Invalid parameter "user_id"');
                return false;
            }

            for (var i = 1; i < paths.length; i++) {
                if (i > 2) {
                    break;
                }

                apiPath += "/" + paths[i];
            }

            switch (apiPath) {
                case '/i/bulk':
                {

                    var requests = queryString.requests,
                        appKey = queryString.app_key;

                    if (requests) {
                        try {
                            requests = JSON.parse(requests);
                        } catch (SyntaxError) {
                            console.log('Parse bulk JSON failed');
                        }
                    } else {
                        common.returnMessage(params, 400, 'Missing parameter "requests"');
                        return false;
                    }

                    for (var i = 0; i < requests.length; i++) {

                        if (!requests[i].app_key && !appKey) {
                            continue;
                        }

                        var tmpParams = {
                            'app_id':'',
                            'app_cc':'',
                            'ip_address':requests[i].ip_address,
                            'user':{
                                'country':requests[i].country_code || 'Unknown',
                                'city':requests[i].city || 'Unknown'
                            },
                            'qstring':{
                                'app_key':requests[i].app_key || appKey,
                                'device_id':requests[i].device_id,
                                'metrics':requests[i].metrics,
                                'events':requests[i].events,
                                'session_duration':requests[i].session_duration,
                                'begin_session':requests[i].begin_session,
                                'end_session':requests[i].end_session,
                                'timestamp':requests[i].timestamp
                            }
                        };

                        if (!tmpParams.qstring.device_id) {
                            continue;
                        } else {
                            tmpParams.app_user_id = common.crypto.createHash('sha1').update(tmpParams.qstring.app_key + tmpParams.qstring.device_id + "").digest('hex');
                        }

                        if (tmpParams.qstring.metrics) {
                            if (tmpParams.qstring.metrics["_carrier"]) {
                                tmpParams.qstring.metrics["_carrier"] = tmpParams.qstring.metrics["_carrier"].replace(/\w\S*/g, function (txt) {
                                    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
                                });
                            }

                            if (tmpParams.qstring.metrics["_os"] && tmpParams.qstring.metrics["_os_version"]) {
                                tmpParams.qstring.metrics["_os_version"] = tmpParams.qstring.metrics["_os"][0].toLowerCase() + tmpParams.qstring.metrics["_os_version"];
                            }
                        }

                        validateAppForWriteAPI(tmpParams);
                    }

                    common.returnMessage(params, 200, 'Success');
                    break;
                }
                case '/i/users':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    /*if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }*/

                    switch (paths[3]) {
                        case 'create':
                            validateUserForSignUp(semusiApi.mgmt.users.createUser, params);
                            break;
                        case 'update':
                            validateUserForWriteAPI(semusiApi.mgmt.users.updateUser, params);
                            break;
                        case 'delete':
                            validateUserForWriteAPI(semusiApi.mgmt.users.deleteUser, params);
                            break;
                        case 'recover':
                            validateUserForWriteAPI(semusiApi.mgmt.users.recover, params);
                            break;
                        case 'updateApproved':
                            validateUserForWriteAPI(semusiApi.mgmt.users.updateApprovedUser, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update or /delete');
                            break;
                    }

                    break;
                }
                case '/i/apps':
                case '/i/appsV2':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'getAppStoreInfo':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.getAppStoreInfo, params);
                            break;
                        case 'getCurrentAppKeys':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.getCurrentAppKeys, params);
                            break;
                        case 'SendDevData':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.SendDevData, params);
                            break
                        case 'processfile':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.processfile, params);
                            break;
                        case 'processUninstallData':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.processUninstallData, params);
                            break;
                            case 'checkemaildomain':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.checkemaildomain, params);
                            break
                        case 'getApiKey':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.getApiKey, params);
                            break
                        case 'updateApp':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateAppNew, params);
                            break
                        case 'updateCallBackUrl':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateCallBackUrl, params);
                            break;
                        case 'deleteCallBackUrl':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteCallBackUrl, params);
                            break;
                        case 'create':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.createApp, params);
                            break;
                        case 'createChatBot':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.createChatBot, params);
                            break;
                        case 'update':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateApp, params);
                            break;
                        case 'delete':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteApp, params);
                            break;
                        case 'reset':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.resetApp, params);
                            break;
                        case 'updateGCMKeys' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateGCMKeys, params);
                            break;
                        case 'updateAppiceKeys' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateAppiceKeys, params);
                            break;
                        case 'updateIOSCertificates' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateIOSCertificates, params);
                            break;
                        case 'updateAppMetaData' : {
                                common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update, /delete or /reset');
                                //This API is called multiple times from older version of SDK and need not to be called now.
                                //validateUserForWriteAPI(semusiApi.mgmt.apps.updateAppMetaData, params);
                                break;
                        }
                        case 'updateDeviceMetaData' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateDeviceMetaData, params);
                            break;
                        case 'setAppMode' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.setAppMode, params);
                            break;
                        case 'saveModule' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.saveModule, params);
                            break;
                        case 'saveAppModules' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.saveAppModules, params);
                            break;
                        case 'updateModule' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateModule, params);
                            break;
                        case 'deleteModule' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteModule, params);
                            break;
                        case 'saveOtherAppGroup' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.saveOtherAppGroup, params);
                            break;
                        case 'updateOtherAppGroup' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateOtherAppGroup, params);
                            break;
                        case 'deleteOtherAppGroup' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteOtherAppGroup, params);
                            break;
                        case 'addAppToGroup' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.addAppToGroup, params);
                            break;
                        case 'updateAppToGroup' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateAppToGroup, params);
                            break;
                        case 'deleteAppFromGroup' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteAppFromGroup, params);
                            break;
                        case 'getAppsAverageSession' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.getAppsAverageSession, params);
                            break;
                        case 'saveChildApp' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.saveChildApp, params);
                            break;
                        case 'updateChildApp' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateChildApp, params);
                            break;
                        case 'deleteChildApp' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteChildApp, params);
                            break;
                        case 'updateLinkingField' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateLinkingField, params);
                            break;
                        case 'saveEmailConfiguration' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.saveEmailConfiguration, params);
                            break;
                        case 'saveSMSConfiguration' :
                            validateUserForWriteAPI(semusiApi.mgmt.apps.saveSMSConfiguration, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update, /delete or /reset');
                            break;
                    }

                    break;
                }
                case '/i/campaigns':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'create':
                            validateUserForWriteAPI(semusiApi.mgmt.campaigns.createCampaign, params);
                            break;
                        case 'update':
                            validateUserForWriteAPI(semusiApi.mgmt.campaigns.updateCampaign, params);
                            break;
                        case 'delete':
                            validateUserForWriteAPI(semusiApi.mgmt.campaigns.deleteCampaign, params);
                            break;
                        case 'run':
                            validateUserForWriteAPI(semusiApi.mgmt.campaigns.runCampaign, params);
                            break;
                        case 'updateEmailStats':
                            //semusiApi.mgmt.netcoreEmail.updateViewStats(params);
                            validateUserForWriteAPI(semusiApi.mgmt.netcoreEmail.updateViewStats, params);
                            break;
                        case 'updateSMSStats':
                            validateUserForWriteAPI(semusiApi.mgmt.smsApi.updateStats, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update, /delete or /reset');
                            break;
                    }

                    break;
                }
                case '/o/campaigns':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'all':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.campaigns.getAllCampaigns, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update, /delete or /reset');
                            break;
                    }

                    break;
                }
                case '/i/gcm':
                case '/i/gcmV2':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'update':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateGCM, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /update');
                            break;
                    }
                    break;
                }
                case '/i':
                case '/i/V2':
                {
                    var ipAddress = req.headers['x-forwarded-for'] || req.connection.remoteAddress;

                    params.ip_address =  params.qstring.ip_address || ipAddress.split(",")[0];
                    params.user = {
                        'country':'Unknown',
                        'city':'Unknown'
                    };

                    if (!params.qstring.app_key || !params.qstring.device_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_key" or "device_id"');
                        return false;
                    } else {
                        // Set app_user_id that is unique for each user of an application.
                        params.app_user_id = common.crypto.createHash('sha1').update(params.qstring.app_key + params.qstring.device_id + "").digest('hex');
                    }

                    if (params.qstring.metrics) {
                        try {
                            params.qstring.metrics = JSON.parse(params.qstring.metrics);

                            if (params.qstring.metrics["_carrier"]) {
                                params.qstring.metrics["_carrier"] = params.qstring.metrics["_carrier"].replace(/\w\S*/g, function (txt) {
                                    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
                                });
                            }

                            if (params.qstring.metrics["_os"] && params.qstring.metrics["_os_version"]) {
                                params.qstring.metrics["_os_version"] = params.qstring.metrics["_os"][0].toLowerCase() + params.qstring.metrics["_os_version"];
                            }

                        } catch (SyntaxError) {
                            console.log('Parse metrics JSON failed');
                            console.log(params.qstring.metrics);
                        }
                    }

                    if (params.qstring.events) {
                        try {
                            params.qstring.events = JSON.parse(params.qstring.events);
                        } catch (SyntaxError) {
                            params.qstring.events =[];
                            console.log('Parse events JSON failed');
                        }
                    }

                    validateAppForWriteAPI(params);

                    if (!common.config.api.safe) {
                        common.returnMessage(params, 200, 'Success');
                    }

                    break;
                }
                case '/i/validatekey':
                {
                    if (!params.qstring.app_key) {
                        common.returnMessage(params, 400, 'Missing parameter "app_key" or "device_id"');
                        return false;
                    }

                    validateAppForWriteAPI(params);

                    if (!common.config.api.safe) {
                        common.returnMessage(params, 200, 'Success');
                    }

                    break;
                }
                case '/o/user':
                {

                    switch (paths[3]) {
                        case 'login':
                            validateUserForLogin(semusiApi.mgmt.users.login,params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /all or /me');
                            break;
                        }
                    break;
                }
                case '/o/appusers':
                {

                    switch (paths[3]) {
                        case 'getAppAndUserData':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getAppAndUserData,params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /all or /me');
                            break;
                        }

                    break;
                }
                case '/o/users':
                {
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'all':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getAllUsers, params);
                            break;
                        case 'me':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getCurrentUser, params);
                            break;
                        case 'allUsers':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getAllUsersNew, params);
                            break;
                        case 'getAppUsers':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getAppUsers, params);
                            break;
                        case 'getUserDetails':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getUserDetails, params);
                            break;
                        case 'getUserRecentActivity':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.users.getUserRecentActivity, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /all or /me');
                            break;
                    }

                    break;
                }
                case '/o/apps':
                case '/o/appsV2':
                {
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'all':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getAllApps, params);
                            break;
                        case 'mine':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getCurrentUserApps, params);
                            break;
                        case 'getGlobalAppModules':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getGlobalAppModules, params);
                            break;
                        case 'getAppModules':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getAppModules, params);
                            break;
                        case 'getActiveModules':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getActiveModules, params);
                            break;
                        case 'getOtherAppGroups':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getOtherAppGroups, params);
                            break;
                        case 'getChildApps':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getChildApps, params);
                            break;
                        case 'getAppDetails':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getAppDetails, params);
                            break;
                        case 'getCompetingApps':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getCompetingApps, params);
                            break;
                        case 'getCompetingAppsAppWise':
                        validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getCompetingAppsAppWise, params);
                        break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /all or /mine');
                            break;
                    }

                    break;
                }
                case '/o':
                {
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (!params.qstring.app_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_id"');
                        return false;
                    }

                    switch (params.qstring.method) {
                        case 'locations':
                        case 'sessions':
                        case 'users':
                        case 'devices':
                        case 'device_details':
                        case 'carriers':
                        case 'gender':
                        case 'age':
                        case 'height':
                        case 'weight':
                        case 'app_versions':
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchTimeData, params.qstring.method);
                            break;
                        case 'cities':
                            if (common.config.api.city_data !== false) {
                                validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchTimeData, params.qstring.method);
                            } else {
                                common.returnOutput(params, {});
                            }
                            break;
                        case 'activity':
                            //override the event data to be fetched
                            params.qstring.events="Activity";
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchMergedEventData);
                            break;
                        case 'places':
                            params.qstring.events="Places";
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchMergedEventData);
                            break;
                        case 'campaignperformance':
                            params.qstring.events="CampaignPerformance";
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchMergedEventData);
                            break;
                        case 'events':
                            if (params.qstring.events) {
                                try {
                                    params.qstring.events = JSON.parse(params.qstring.events);
                                } catch (SyntaxError) {
                                    console.log('Parse events array failed');
                                }

                                validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchMergedEventData);
                            } else {
                                validateUserForDataReadAPI(params, semusiApi.data.fetch.prefetchEventData, params.qstring.method);
                            }
                            break;
                        case 'get_events':
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchCollection, 'events');
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid method');
                            break;
                    }

                    break;
                }
                case '/i/aud':
                        if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (!params.qstring.app_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_id"');
                        return false;
                    }

                    switch (paths[3]){
                        case 'create':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.createAudSegment, params);
                            break;
                        case 'update':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.updateAudSegment, params);
                            break;
                        case 'delete':
                            validateUserForWriteAPI(semusiApi.mgmt.apps.deleteAudSegment, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update, /delete');
                            break;
                    }
                break;
                case '/o/aud':
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (!params.qstring.app_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_id"');
                        return false;
                    }

                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    switch (params.qstring.method) {
                        case 'get_metacustomvariables':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getMetaCustomVariableFields, params);
                            break;
                        case 'get_metacustomevents':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getMetaCustomEventFields, params);
                            break;
                        case 'countUserBase':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.countUserBase, params);
                            break;
                        case 'countAudienceReach':
                            validateUserForMgmtReadAPI(semusiApi.mgmt.apps.countAudienceReach, params);
                            break;
                        case 'getAudienceSegments':
                                validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getAudienceSegments, params);
                            break;
                        case 'getAudienceSegmentById':
                                validateUserForMgmtReadAPI(semusiApi.mgmt.apps.getAudienceSegmentById, params);
                            break;
                        case 'get_metavariables':
                                validateUserForMgmtReadAPI(semusiApi.mgmt.apps.get_metavariables, params);
                            break;
                        case 'validateSegmentName':
                                validateUserForMgmtReadAPI(semusiApi.mgmt.apps.validateSegmentName, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid method');
                            break;
                    }
                break;
                case '/o/analytics':
                {
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (!params.qstring.app_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_id"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'dashboard':
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchDashboard);
                            break;
                        case 'countries':
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchCountries);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /dashboard or /countries');
                            break;
                    }

                    break;
                }
                case '/o/demographics':
                {
                    //get the demogrpahics against the given deviceid
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (!params.qstring.device_id) {
                        common.returnMessage(params, 400, 'Missing parameter "device_id"');
                        return false;
                    }
                    validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchDemographics);
                    break;
                }
                case '/i/metrices':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }


                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }
                    switch (paths[3]) {
                        case 'createreferal':
                            validateUserForWriteAPI(semusiApi.mgmt.metrices.createReferal, params);
                            break;
                        case 'deletereferal':
                            validateUserForWriteAPI(semusiApi.mgmt.metrices.deleteReferal, params);
                            break;
                    }
                    break;
                }
                case '/o/metrices':
                {
                    //get the app install/uninstall metrices
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }
                        if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if(paths[3])
                    {
                        switch(paths[3]){
                            case 'getAllReferals':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getReferals);
                            break;
                            case 'getReferalMappings':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getReferalMappings);
                            break;
                            case 'getCSVLogs':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getCSVLogs);
                            break;
                            case 'getReferralCounts':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getReferralCounts);
                            break;
                            case 'getCohortDataForInstalls':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getCohortDataForInstalls);
                            break;
                            case 'getUsersAtRisk':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getUsersAtRisk);
                            break;
                            case 'getAppStats':
                            validateUserForAppStats(params, semusiApi.mgmt.metrices.getAppStats);
                            break;
                            case 'getEventsStats':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getEventsStats);
                            break;
                            case 'getEventAttributeStats':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getEventAttributeStats);
                            break;
                            case 'getEventList':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getEventList);
                            break;
                            case 'getEventExploreData':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getEventExploreData);
                            break;
                            case 'getTables':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getTables);
                            break;
                            case 'getEventSchema':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getEventSchema);
                            break;
                            case 'getEventRawData':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getEventRawData);
                            break;
                            case 'getUsagesRetention':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getUsagesRetention);
                            break;
                            case 'getReturningUsersCount':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getReturningUsersCount);
                            break;
                            case 'downloadData':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.downloadData);
                            break;
                            case 'downloadDataCustom':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.downloadDataCustom);
                            break;
                            case 'downloadEventData':
                            validateUserForDataReadAPI(params, semusiApi.data.events.downloadEventData);
                            break;
                            case 'getTotalStats':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getDashboardTotalStats);
                            break;
                            case 'getCampaignStats':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getCampaignStats);
                            break;
                            case 'getInstallUnistallStatsByParam':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getInstallUnistallStatsByParam);
                            break;
                            case 'getInstallUnistallLocation':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getInstallUnistallLocation);
                            break;
                            case 'getActiveUserSessionStats':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserSessionStats);
                            break;
                            case 'getActiveUserSessionStatsDashboard':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserSessionStatsDashboard);
                            break;
                            case 'getActiveUserTimelySessions':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserTimelySessions);
                            break;
                            case 'getDauWauMau':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getDauWauMau);
                            break;
                            case 'getDauWauMauWithPlatform':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserDauAndMauWithPlatforms);
                            break;
                            case 'getActiveUserInterests':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserInterests);
                            break;
                            case 'getActiveUserDemographics':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserDemographics);
                            break;
                            case 'getActiveUserCompetingAppStats':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUserCompetingAppStats);
                            break;
                            case 'getRegainedUsers':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getRegainedUsers);
                            break;
                            case 'getInstallUnistallStatsBySource':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getInstallUnistallStatsBySource);
                            break;
                                case 'getActiveUsers':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getActiveUsers);
                            break;
                            case 'getChatBotSession':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.metrices.getChatBotSession);
                            break;
                            case 'getChatBotCohort':
                            validateUserForDataReadAPI(params, semusiApi.data.events.getChatBotCohort);
                            break;
                            default:
                            validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchTimeData,"install_counter");
                            break;
                    }
                    }
                    else
                    {
                        validateUserForDataReadAPI(params, semusiApi.data.fetch.fetchTimeData,"install_counter");
                    }
                    break;
                }
                case '/i/templates':
                case '/i/templatesV2':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    switch (paths[3]) {
                        case 'create':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.createTemplate, params);
                            break;
                        case 'update':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.updateTemplate, params);
                            break;
                        case 'delete':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.deleteTemplate, params);
                            break;
                        case 'response':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.updateResponse, params);
                            break;
                        case 'responseTest':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.updateResponseTest, params);
                            break;
                        case 'bulkResponse':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.updateBulkResponse,params);
                            break;
                        case 'createCampaign':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.createCampaign, params);
                            break;
                        case 'updateCampaign':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.updateCampaign, params);
                            break;
                        case 'setActiveCampaign':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.setActiveCampaign, params);
                            break;
                        case 'deleteCampaign':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.deleteCampaign, params);
                            break;
                        case 'copyCampaign':
                                validateUserForWriteAPI(semusiApi.mgmt.templates.copyCampaign, params);
                            break;
                        case 'uploadImage':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.uploadImage, params);
                            break;
                        case 'getTransitionalCampaigns':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.getTransitionalCampaigns, params);
                            break;
                        case 'getCampaignDetails':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.getCampaignDetails, params);
                            break;
                        case 'downloadTransitionalCampaigns':
                            validateUserForWriteAPI(semusiApi.mgmt.templates.downloadTransitionalCampaigns, params);
                            break;
                        default:
                            common.returnMessage(params, 400, 'Invalid path, must be one of /create, /update, /delete or /response');
                            break;
                    }

                    break;
                }
                case '/o/templates':
                case '/o/templatesV2':
                {
                    //get the app saved templates
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                        if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    switch(paths[3]){
                        case 'getAllTemplates':
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getTemplates);
                            break;
                        case "getTemplatesById":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getTemplatesById);
                            break;
                        case "getTemplatesByType":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getTemplatesByType);
                            break;
                        case "getCampaigns":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getCampaigns);
                            break;
                        case "getActiveCampaigns":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getActiveCampaigns);
                            break;
                        case "getCampaignsByStatus":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getCampaignsByStatus);
                            break;
                        case "getCampaignById":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getCampaignById);
                            break;
                        case "getPastAndActiveCampaigns":
                            validateUserForDataReadAPI(params, semusiApi.mgmt.templates.getPastAndActiveCampaigns);
                            break;
                        case "getCampaignFeedbackById":
                            validateUserForDataReadAPI(params,semusiApi.mgmt.templates.getCampaignFeedbackById);
                            break;
                        case "getCampaignReachById":
                            validateUserForDataReadAPI(params,semusiApi.mgmt.templates.getCampaignReachById);
                            break;
                        case "getAtRiskAudienceSegmentId":
                            validateUserForDataReadAPI(params,semusiApi.mgmt.templates.getAtRiskAudienceSegmentId);
                            break;
                        case "downloadActiveCampaignData":
                            validateUserForDataReadAPI(params,semusiApi.mgmt.templates.downloadActiveCampaignData);
                            break;
                    }
                    break;

                }
                case '/o/places':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }
                        switch(paths[3]){
                        case 'getPlaces':
                            semusiApi.data.places.getPlaces(params);
                        break;
                    }
                    break;
                }
                case '/i/backup':
                {
                    if(req.method == 'POST') {
                        //common.returnMessage(params, 200, 'true');
                        processPost(req, res, params, function(data) {
                            //semusiApi.data.awsHelper.sendDataToKinesis(data);
                            //semusiApi.data.kafkaProducer.sendDataToKafka(data);
                            sendBackupDataToQueue(data,params);
                            common.returnMessage(params, 200, 'true');
                            return true;
                        });
                    }
                    break;
                }
                case '/i/backupV2':
                {
                    if(req.method == 'POST') {
                        //common.returnMessage(params, 200, 'true');
                        processPostV1(req, res, params, function(data) {
                            //semusiApi.data.awsHelper.sendDataToKinesis(data);
                            //semusiApi.data.kafkaProducer.sendDataToKafka(data);
                            sendBackupDataToQueue(data,params);
                            common.returnMessage(params, 200, 'true');
                            return true;
                        });
                    }
                    break;
                }
                case '/i/backupTest':
                {
                    semusiApi.data.backup.siegeTest(params);
                    break;
                }
                case '/i/webhook':
                {
                    semusiApi.data.backup.siegeTest(params);
                    break;
                }
                case '/i/screenshot':
                {
                    //get the app install/uninstall metrices
                    if (!params.qstring.app_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_id"');
                        return false;
                    }
                        if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if(paths[3])
                    {
                        switch(paths[3]){
                            case 'userHistory':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.userHistory);
                            break;
                            case 'currentUsers':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.currentUsers);
                            break;
                            case 'getCurrentSession':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.getCurrentSession);
                            break;
                            case 'getPastSession':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.getPastSession);
                            break;
                        }
                    }
                    break;
                }
                case '/o/screenshot':
                {
                    //get the app install/uninstall metrices
                    if (!params.qstring.app_id) {
                        common.returnMessage(params, 400, 'Missing parameter "app_id"');
                        return false;
                    }
                        if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }

                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    if(paths[3])
                    {
                        switch(paths[3]){
                            case 'setupNewScreenshot':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.setupNewScreenshot);
                            break;
                            case 'deleteScreenshot':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.deleteScreenshot);
                            break;
                            case 'changeSetupScreenshotsStatus':
                            validateUserForDataReadAPI(params, semusiApi.data.screenshot.changeSetupScreenshotsStatus);
                            break;
                        }
                    }
                    break;
                }
                case '/i/tracking':
                {
                    var ipAddress = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
                    params.ip_address =  params.qstring.ip_address || ipAddress.split(",")[0];

                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }
                    switch (paths[3]) {
                        case 'recordClick':
                            semusiApi.data.tracking.recordClick(params);
                            break;
                        case 'addPromoter':
                            semusiApi.data.tracking.addPromoter(params);
                            break;
                        case 'updatePromoter':
                            semusiApi.data.tracking.updatePromoter(params);
                            break;
                        case 'addPromoterCampaign':
                            semusiApi.data.tracking.addPromoterCampaign(params);
                            break;
                        case 'updatePromoterCampaign':
                            semusiApi.data.tracking.updatePromoterCampaign(params);
                            break;
                        case 'referralcallback':
                        //check if the url contains the channel name
                        if(params.qstring.channel == 'appsflyer')
                        {
                                if(req.method == 'POST') {
                                var body='';
                                req.on('data', function (data) {
                                    body +=data;
                                    if(body.length > 1e6) {
                                        body = "";
                                        res.writeHead(413, {'Content-Type': 'text/plain'}).end();
                                        req.connection.destroy();
                                    }
                                });
                                req.on('end',function(){
                                    postData =  JSON.parse(body);
                                    postData.APPID = params.qstring.app_id;
                                    postData.APPKEY = params.qstring.app_key;
                                    postData.APIKEY = params.qstring.api_key;
                                    createCallBackLog(postData,params.qstring.channel+"_"+params.qstring.app_id);
                                    semusiApi.data.tracking.callbackConfirmed(params,params.qstring.channel,postData);
                                });
                            }
                        }
                        else if(params.qstring.channel == 'apsalar')
                        {
                                if(req.method == 'POST') {
                                var body='';
                                req.on('data', function (data) {
                                    body +=data;
                                    if(body.length > 1e6) {
                                        body = "";
                                        res.writeHead(413, {'Content-Type': 'text/plain'}).end();
                                        req.connection.destroy();
                                    }
                                });
                                req.on('end',function(){
                                    postData =  JSON.parse(body);
                                    postData.APPID = params.qstring.app_id;
                                    postData.APPKEY = params.qstring.app_key;
                                    postData.APIKEY = params.qstring.api_key;
                                    createCallBackLog(postData,params.qstring.channel+"_"+params.qstring.app_id);
                                    semusiApi.data.tracking.callbackConfirmed(params,params.qstring.channel,postData);
                                });
                            }
                        }
                        else if(params.qstring.channel == 'google_adwords')
                        {
                            if(req.method == 'POST') {
                                var body='';
                                req.on('data', function (data) {
                                    body +=data;
                                    if(body.length > 1e6) {
                                        body = "";
                                        res.writeHead(413, {'Content-Type': 'text/plain'}).end();
                                        req.connection.destroy();
                                    }
                                });
                                req.on('end',function(){
                                    postData =  JSON.parse(body);
                                    postData.APPID = params.qstring.app_id;
                                    postData.APPKEY = params.qstring.app_key;
                                    postData.APIKEY = params.qstring.api_key;
                                    createCallBackLog(postData,params.qstring.channel+"_"+params.qstring.app_id);
                                    //semusiApi.data.tracking.callbackConfirmed(params,params.qstring.channel,postData);
                                    common.returnMessage(params, 200, 'success');
                                    return true;
                                });
                            }
                            else if(req.method == 'GET') {
                                var postData = {};
                                postData = params.qstring;
                                postData.type = 'GET';
                                createCallBackLog(postData,params.qstring.channel+"_"+params.qstring.app_id);
                                common.returnMessage(params, 200, 'success');
                                return true;
                            }
                        }
                        else if(params.qstring.channel == 'mat' || params.qstring.channel == 'tune')
                        {
                                if(req.method == 'POST') {
                                semusiApi.data.tracking.callbackConfirmed(params,params.qstring.channel,params.qstring);
                            }
                        }
                        else
                        {
                            common.returnMessage(params, 404, 'channel not found');
                            return true;
                        }
                        break;
                    }

                    break;
                }
                case '/o/tracking':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }
                    switch (paths[3]) {
                        case 'getPromotersByMember':
                            semusiApi.data.tracking.getPromotersByMember(params);
                            break;
                        case 'getPromotersById':
                            semusiApi.data.tracking.getPromotersById(params);
                            break;
                        case 'getMemberApps':
                            semusiApi.data.tracking.getMemberApps(params);
                            break;
                        case 'getPromoterCampaigns':
                            semusiApi.data.tracking.getPromoterCampaigns(params);
                            break;
                        case 'getPromoterCampaignById':
                            semusiApi.data.tracking.getPromoterCampaignById(params);
                            break;
                        case 'getClickRecords':
                            semusiApi.data.tracking.getClickRecords(params);
                            break;
                    }

                    break;
                }
                case '/i/getClientIp':
                {
                    var ipAddress = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
                    ipAddress = ipAddress.split(",")[0];
                        common.returnMessage(params, 200, ipAddress);
                        return true;
                        break;
                }
                case '/o/profile':
                case '/o/profileV2':
                {
                        if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }

                    validateUserForMgmtReadAPI(semusiApi.mgmt.users.getUserProfile, params);
                    return true;
                    break;
                }
                case '/i/funnels':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }
                    switch (paths[3]) {
                        case 'create':
                            validateUserForWriteAPI(semusiApi.data.events.createFunnel, params);
                            break;
                        case 'update':
                            validateUserForWriteAPI(semusiApi.data.events.updateFunnel, params);
                            break;
                        case 'delete':
                            validateUserForWriteAPI(semusiApi.data.events.deleteFunnel, params);
                            break;
                    }
                    break;
                }
                case '/o/funnels':
                {
                    if (params.qstring.args) {
                        try {
                            params.qstring.args = JSON.parse(params.qstring.args);
                        } catch (SyntaxError) {
                            console.log('Parse ' + apiPath + ' JSON failed');
                        }
                    }
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }
                    switch (paths[3]) {
                        case 'getEventFunnels':
                            validateUserForWriteAPI(semusiApi.data.events.getEventFunnels, params);
                            break;
                        case 'stats':
                            validateUserForWriteAPI(semusiApi.data.events.getEventFunnelStats, params);
                            break;
                        case 'validateFunnelName':
                            validateUserForWriteAPI(semusiApi.data.events.validateFunnelName, params);
                            break;
                    }
                    break;
                }

                case '/i/emailTracking':
                {
                    if (!params.qstring.api_key) {
                        common.returnMessage(params, 400, 'Missing parameter "api_key"');
                        return false;
                    }
                    switch (paths[3]) {
                        case 'octaneEmail':
                            validateUserForWriteAPI(semusiApi.data.emailTracking.octaneEmail, params);
                            break;
                    }
                    break;
                }

            }
        }
}
