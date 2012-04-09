var sys = require('sys'),
    postmark = require('postmark-api')("APIKEYHERE"),
    Client = require('mysql').Client,
    client = new Client();
 
client.user = 'singleyet';
client.password = '***REMOVED***';

client.connect(function(error, results) {
    if(!error) {
        setTable(client);
    }
});

setTable = function(client)
{
    client.query('USE singleyet', function(error, results) {
        if(error) {
            console.log('Error: ' + error.message);
            client.end();
            return;
        }
        grabData(client);
    });
};

grabData = function(client)
{
    client.query('SELECT * FROM follow', function iterate(error, results, fields) {
        if(results.length > 0)
        {
            for (var i = 0; i < results.length; i++)
            {
                checkResult(results[i]);
            }
        }
    });
    client.end();
};

checkResult = function(result)
{
    //connect to fb and check for new relationship
}