var util = require('util'),
    postmark = require('postmark-api')("APIKEYHERE"),
    mysql = require('mysql'),
    FacebookClient = require("facebook-client").FacebookClient

var client = mysql.createClient({
    database: 'singleyet',
    //user: 'singleyet',
    //password: '***REMOVED***'
    user: 'root',
    password: 'yd3k',
    host: '10.0.30.2'
});

client.query('SELECT * FROM followed', function iterate(error, results, fields) {
    if(results.length > 0)
    {
        for (var i = 0; i < results.length; i++)
        {
            checkResult(results[i]);
        }
    }
});
client.end();

checkResult = function(result)
{
    //connect to fb and check for new relationship
    console.log(result);
}