var util = require('util'),
    postmark = require('postmark-api')("APIKEYHERE"),
    mysql = require('mysql'),
    FacebookClient = require("facebook-client").FacebookClient,
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
    );

singleYet = function(){
    client.query('SELECT * FROM `followed` JOIN `user` on followed.user_id = user.id', function iterate(error, results, fields) {
        if(results.length > 0)
        {
            for (var i = 0; i < results.length; i++)
            {
                checkResult(results[i]);
            }
        }
    });
    //client.end();
}


checkResult = function(db_result)
{
    //connect to fb and check for new relationship
    console.log(db_result);
    
    params = {
        'access_token': db_result['token']
    };
    
    fb_client.graphCall('/me', params)(function(fb_result) {
        var rel_status = 1; //testing
        if (rel_status != db_result['rel_status']) //(if fb_result['relationship'] != result['rel_status'])
        {
            //got a change, push notification and send email, then change db
            pushNotification(db_result, fb_result);
            sendEmail(db_result, fb_result);
            
            //set new rel_status on table
            client.query(
                'UPDATE `followed` '+
                'SET rel_status = ? '+
                'WHERE id = ?',
                [1, db_result['id']] //change me!
            );
            
        }
    });
}

pushNotification = function(db_data, fb_data)
{
    //add row to notificaitons table
    client.query(
      'INSERT INTO `notification`'+
      'SET user_id = ?, followed_id = ?, message = ?',
      [db_data['user_id'], db_data['followed_id'], 'so and so is now in a relationship' ]
    );
}

sendEmail = function(db_data, fb_data)
{
    var to = db_data['email'],
        subject = 'so and so is now in a relationship',
        boby = '';
    
    postmark.send({
        "From": "webmaster@singleyet.com",
        "To": to,
        "Subject": subject,
        "TextBody": body
    });
}

singleYet();