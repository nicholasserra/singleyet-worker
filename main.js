var util = require('util'),
    postmark = require('postmark-api')("***REMOVED***"),
    mysql = require('mysql'),
    FacebookClient = require("facebook-client").FacebookClient,
    raven = require('raven'),
    client = mysql.createClient({
        database: 'singleyet',
        user: 'singleyet',
        password: '***REMOVED***'
        //user: 'root',
        //password: 'root',
        //host: '10.0.100.32'
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
    console.log('Got results from db on relationship statuses');
    if(results.length > 0)
    {
        console.log('Got relationship statuses');
        for (var i = 0; i < results.length; i++)
        {
            console.log(results[i]['name']);
            relationship_codes[results[i]['name']] = results[i]['id'];
        }
        singleYet();
    }
});

singleYet = function(){
    console.log('in main singleyet function');
    client.query('SELECT user_id, followed.fb_id, rel_status_id, access_token, email, followed.id AS followed_id FROM `followed` JOIN `user` on followed.user_id = user.id', function iterate(error, results) {
        
        console.log('in client.query main function');
        
        if(results.length > 0){
            console.log('got results from followed table');
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
                    sorted.push({'user_id': results[i]['user_id'], 'email': results[i]['email'], 'access_token': results[i]['access_token'], 'following': [{'fb_id': results[i]['fb_id'], 'rel_status': results[i]['rel_status'], 'followed_id': results[i]['followed_id']}]});
                }
            }
            
            console.log('after main push loop');
            
            for (var i = 0; i < sorted.length; i++){
                console.log('in loop to check result');
                checkResult(sorted[i], function(){
                    if (i == sorted.length-1 && jobs == 0){
                        //no jobs after for loop exhausted and all checks done
                        console.log('no jobs after check loop done');
                        client.end()
                    }
                })
            }
        }
        else{
            //no results
            console.log('no followed results');
            client.end();
        }
    });
}

checkResult = function(user_data, callback){
    console.log('in check result');
    params = {
        'access_token': user_data['access_token']
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

                var message = parsed_body['name']+' is now '+parsed_body['relationship_status'];
                
                pushNotification(user_data['user_id'], db_followed_id, message, parseInt(relationship_codes[parsed_body['relationship_status']]));
                updateRow(db_followed_id, parseInt(relationship_codes[parsed_body['relationship_status']]));
                
                email_stories.push(parsed_body['name']+' is now '+parsed_body['relationship_status']);
            }
            else if(!('relationship_status' in parsed_body) &&
                parseInt(relationship_codes['Not set']) != parseInt(db_rel_status)){
                
                //got a change from being listed to not listed
                jobs = jobs + 2;
                
                var message = parsed_body['name']+' has hidden their relationship status.';

                pushNotification(user_data['user_id'], db_followed_id, message, parseInt(relationship_codes['Not set']));
                updateRow(db_followed_id, parseInt(relationship_codes['Not set']));
                
                email_stories.push(parsed_body['name']+' has hidden their relationship status');
            }
        }
        jobs++;
        sendEmail(user_data, email_stories);

        callback();
    });
}

pushNotification = function(user_id, followed_id, message, rel_status){
    console.log('add notification');
    //add row to notificaitons table
    client.query(
        'INSERT INTO `notification`'+
        'SET user_id = ?, followed_id = ?, message = ?, rel_status = ?, timestamp = ?',
        [user_id, followed_id, message, rel_status, Math.round((new Date()).getTime() / 1000)],
        function(){
            subtractAndCheck();
        }
    );
}

sendEmail = function(user_data, stories){
    console.log('send an email');
    if (user_data['email_opt']){
        var to = user_data['email'],
            subject = (stories.length == 1) ? fb_data['name']+' is now '+fb_data['relationship_status'] : "You have Single Yet notifications", 
            body = stories.join('\n');
        
        console.log('send email to '+to)
        postmark.send({
            "From": "webmaster@singleyet.com",
            "To": to,
            "Subject": subject,
            "TextBody": body
        }, function(err, res){
            console.log(err);
            console.log(response);
            console.log('email callback');
            subtractAndCheck();
        });
    }
    else{
        console.log('opt out of email');
        subtractAndCheck();
    }
}

updateRow = function(id, rel_status){
    console.log('update followed row');
    //set new rel_status on table
    client.query(
        'UPDATE `followed` '+
        'SET rel_status = ? '+
        'WHERE id = ?',
        [rel_status, id],
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
