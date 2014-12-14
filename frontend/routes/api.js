/**
 * Created by feiguan on 12/12/14.
 */

var log4js = require('log4js');
var logger = log4js.getLogger('API');
var responses = require('../models/response');
var http = require('http');
var url = require('url');

getJSON = function (options, onResult) {

    var res = http.request(options,function(res){

        var output = '';
        res.on('data', function (chunk) {
            output += chunk;
        });

        res.on('end', function () {
            var obj = JSON.parse(output);
            onResult(res.statusCode, obj);
        });

    });

    res.end();
}

exports.search = function (req, res) {
    var queryObject = url.parse(req.url,true).query;
    var params = "";
    Object.keys(queryObject).forEach(function (ele, idx, arr) {
        if (idx != 0)
            params = params.concat("&");
        params = params.concat(encodeURIComponent(ele)).concat("=").concat(encodeURIComponent(queryObject[ele]));
    });
    var options = {
        host: 'localhost',
        port: 25801,
        path: '/search?'.concat(params),
        method: 'GET',
        headers: {
            accept: 'application/json'
        }
    };
    logger.info("sending request to: " + options.host + ":" + options.port.toString() + options.path);
    getJSON(options, function (statusCode, result) {

        res.statusCode = statusCode;
        res.send(result);
    });
}
