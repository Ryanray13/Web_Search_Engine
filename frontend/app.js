/**
 * Created by feiguan on 12/12/14.
 */

var log4js = require('log4js');
var logger = log4js.getLogger('Application');
GLOBAL.systemLogger = logger;
logger.info("Server starting");

var express = require('express');
var app = express();
app.use(express.bodyParser());
app.use(app.router);
app.use(express.static(__dirname));

var api = require('./routes/api');

app.get('/search', api.search);

var port = process.env.PORT || 3000;
app.listen(port);

logger.info("Server started, running at http://localhost:" + port);