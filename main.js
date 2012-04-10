var util = require('util'),
    postmark = require('postmark-api')("***REMOVED***"),
    mysql = require('mysql'),
    FacebookClient = require("facebook-client").FacebookClient,
    raven = require('raven'),
    client = mysql.createClient({
        database: 'singleyet',
        //user: 'singleyet',
        //password: '***REMOVED***'
        user: 'root',
        password: 'yd3k',
        host: '10.0.30.2'
    }),
    fb_client = new FacebookClient(
        "***REMOVED***",
        "***REMOVED***",
        {
            "timeout": 10000
        }
    ),
    relationship_codes = {},
    jobs = 0;

raven.patchGlobal('***REMOVED***');
    
//load relationship statusees
client.query('SELECT * FROM `rel_status`', function iterate(error, results, fields) {
    if(results.length > 0)
    {
        for (var i = 0; i < results.length; i++)
        {
            relationship_codes[results[i]['name']] = results[i]['id'];
        }
    }
});

singleYet = function(){
    client.query('SELECT * FROM `followed` JOIN `user` on followed.user_id = user.id', function iterate(error, results) {
        if(results.length > 0){

            for (var i = 0; i < results.length; i++)
            {
                checkResult(results[i], function{
                    if (i == results.length-1 && jobs == 0){
                        //no jobs after for loop exhausted and all checks done
                        client.end()
                    }
                });
            }
        }
        else{
            //no results
            client.end();
        }
    });
}

checkResult = function(db_result, callback){
    params = {
        'access_token': db_result['token']
    };

    //connect to fb and check for new relationship 
    fb_client.graphCall('/'+db_result['fb_id'], params)(function(fb_result) {

        if ('relationship_status' in fb_result && 
            fb_result['relationship_status'] in relationship_codes && 
            parseInt(relationship_codes[fb_result['relationship_status']]) != parseInt(db_result['rel_status'])){

            //got a change, push notification and send email, then change db

            jobs = jobs + 3;

            pushNotification(db_result, fb_result);
            sendEmail(db_result, fb_result);
            updateRow(db_result, fb_result);
        }
    });

    callback();
}

pushNotification = function(db_data, fb_data){
    //add row to notificaitons table
    client.query(
        'INSERT INTO `notification`'+
        'SET user_id = ?, followed_id = ?, message = ?, rel_status = ?',
        [db_data['user_id'], db_data['id'], fb_data['name']+' is now '+fb_data['relationship_status'], parseInt(relationship_codes[fb_data['relationship_status']])],
        function(){
            subtractAndCheck();
        }
    );
}

sendEmail = function(db_data, fb_data){

    var to = db_data['email'],
        subject = fb_data['name']+' is now '+fb_data['relationship_status'],
        body = fb_data['name']+' is now '+fb_data['relationship_status'];

    postmark.send({
        "From": "webmaster@singleyet.com",
        "To": to,
        "Subject": subject,
        "TextBody": body
    }, function(){
        subtractAndCheck();
    });
}

updateRow = function(db_data, fb_data){

    //set new rel_status on table
    client.query(
        'UPDATE `followed` '+
        'SET rel_status = ? '+
        'WHERE id = ?',
        [parseInt(relationship_codes[fb_data['relationship_status']]), db_data['id']],
        function(){
            subtractAndCheck();
        }
    );
}

subtractAndCheck = function(){
    jobs--;
    if (jobs == 0){
        client.end();
    }
}

singleYet();