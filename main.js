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

//raven.patchGlobal('***REMOVED***');

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
    client.query('SELECT user_id, fb_id, rel_status, token, email, followed.id AS followed_id FROM `followed` JOIN `user` on followed.user_id = user.id', function iterate(error, results) {
        if(results.length > 0){
            var sorted = [];

            for (var i = 0; i < results.length; i++){
                var in_it = false;
                
                for (var count = 0; count < sorted.length; count++){
                    if (sorted[count]['user_id'] == results[i]['user_id']){
                        //already in list, append
                        in_it = true
                        break;
                    }
                }

                if (in_it){
                    sorted[count]['following'].push({'fb_id': results[i]['fb_id'], 'rel_status': results[i]['rel_status'], 'followed_id': results[i]['followed_id']});
                }
                else{
                    sorted.push({'user_id': results[i]['user_id'], 'email': results[i]['email'], 'token': results[i]['token'], 'following': [{'fb_id': results[i]['fb_id'], 'rel_status': results[i]['rel_status'], 'followed_id': results[i]['followed_id']}]});
                }
            }
            
            console.log(sorted[0]['following']);
            for (var i = 0; i < sorted.length; i++){
                checkResult(sorted[i], function(){
                    //end client check
                    if (i == sorted.length-1 && jobs == 0){
                        //no jobs after for loop exhausted and all checks done
                        client.end()
                    }
                })
            }
        }
        else{
            //no results
            client.end();
        }
    });
}

checkResult = function(user_data, callback){
    params = {
        'access_token': user_data['token']
    };

    params['batch'] = [];

    for (var i = 0; i < user_data['following'].length; i++)
    {
        params['batch'].push({'method': 'GET', 'relative_url': '/'+user_data['following'][i]['fb_id']})
    }

    //connect to fb and check for new relationship 
    fb_client.graphCall('/', params, "POST")(function(fb_results) {

        var email_stories = [];

        for (var i = 0; i < fb_results.length; i++){
            var parsed_body = JSON.parse(fb_results[i]['body']);

            //look up which fb_id we're using
            for (var count = 0; count < user_data['following'].length; count++){
                if (user_data['following'][count]['fb_id'] == parsed_body['id']){
                    //got the id, return DB relationship result
                    //if this gets exhausted then we fucked up
                    db_rel_status = user_data['following'][count]['rel_status'];
                    db_followed_id = user_data['following'][count]['followed_id'];
                    break;
                }
            }

            if ('relationship_status' in parsed_body && 
                parsed_body['relationship_status'] in relationship_codes && 
                parseInt(relationship_codes[parsed_body['relationship_status']]) != parseInt(db_rel_status)){

                //got a change, push notification and send email, then change db
                jobs = jobs + 2;

                pushNotification(db_followed_id, user_data, parsed_body);
                updateRow(db_followed_id, parsed_body);
                
                email_stories.push(parsed_body['name']+' is now '+parsed_body['relationship_status']);
            }
        }
        //jobs++;
        //sendEmail(user_data, email_stories);
    });

    callback();
}

pushNotification = function(followed_id, user_data, fb_data){
    //add row to notificaitons table
    console.log(followed_id);
    console.log(user_data);
    console.log(fb_data);
    client.query(
        'INSERT INTO `notification`'+
        'SET user_id = ?, followed_id = ?, message = ?, rel_status = ?',
        [user_data['user_id'], followed_id, fb_data['name']+' is now '+fb_data['relationship_status'], parseInt(relationship_codes[fb_data['relationship_status']])],
        function(){
            subtractAndCheck();
        }
    );
}

sendEmail = function(user_data, stories){
    var to = user_data['email'],
        subject = (stories.length == 1) ? fb_data['name']+' is now '+fb_data['relationship_status'] : "You have Single Yet notifications", 
        body = stories.join('\n');

    postmark.send({
        "From": "webmaster@singleyet.com",
        "To": to,
        "Subject": subject,
        "TextBody": body
    }, function(){
        subtractAndCheck();
    });
}

updateRow = function(id, fb_data){
    //set new rel_status on table
    client.query(
        'UPDATE `followed` '+
        'SET rel_status = ? '+
        'WHERE id = ?',
        [parseInt(relationship_codes[fb_data['relationship_status']]), id],
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