#!/usr/bin/env python
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from facebook import GraphAPI, FacebookClientError
import simplejson as json

from postmark import PMMail

import time
import datetime

import threading
import Queue

queue = Queue.Queue()

POSTMARK_API_KEY = '***REMOVED***'

from raven import Client
client = Client('***REMOVED***')

MYSQL_USERNAME = 'singleyet'
MYSQL_PASSWORD = '***REMOVED***'
MYSQL_HOST = 'localhost'

'''
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'yd3k'
MYSQL_HOST = '10.0.30.2'
'''

today = datetime.datetime.today()

Base = declarative_base()

#connect to db
engine = create_engine('mysql://%s:%s@%s/singleyet' % (MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST))
connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

relationship_codes = {}

class Notification(Base):
    __tablename__ = 'notification'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    followed_id = Column(Integer)
    message = Column(String)
    rel_status_id = Column(Integer)
    timestamp = Column(Integer)
    fb_id = Column(Integer)
    
class Followed(Base):
    __tablename__ = 'followed'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    fb_id = Column(String)
    rel_status_id = Column(Integer)

def main():

    #grab and sort relationship codes
    rel_result = connection.execute("SELECT * FROM `rel_status`")


    #gotta be a way to map this
    for row in rel_result:
        relationship_codes[row['name']] = row['id']

    #grab and sort following results
    db_result = connection.execute("SELECT user_id, followed.fb_id, rel_status_id, \
                                 access_token, email, \
                                 followed.id AS followed_id, email_opt \
                                 FROM `followed` JOIN `user` on \
                                 followed.user_id = user.id")

    #build our new sorted list
    #push in follower if user is already in list
    srted = []
    for row in db_result:
        in_it = False

        for item in srted:
            if item['user_id'] == row['user_id']:
                #in the sorted array so append followed person
                in_it = True
                break

        if in_it:
            item['following'].append({
                'fb_id': row['fb_id'],
                'rel_status_id': row['rel_status_id'],
                'followed_id': row['followed_id']
            })

        else:
            srted.append({
                'user_id': row['user_id'],
                'email': row['email'],
                'access_token': row['access_token'],
                'email_opt': row['email_opt'],
                'following': [{
                        'fb_id': row['fb_id'],
                        'rel_status_id': row['rel_status_id'],
                        'followed_id': row['followed_id']
                }]
            })

    #make our batch graph call to facebook
    for item in srted:
        queue.put(item)


def worker():
    while 1:
        item = queue.get()
        print 'job running'
        #print item

        email_stories = []

        params = {'access_token': item['access_token']}
        graph = GraphAPI(item['access_token'])

        #full list of following ids
        unchunked = [following['fb_id'] for following in item['following']]

        #divide full list into array of arrays of 50
        chunks = [unchunked[i:i+49] for i in range(0, len(unchunked), 49)]

        #make graph call for each set of 50
        for chunk in chunks:

            params['batch'] = json.dumps([{'method': 'GET', 'relative_url': '/'+c} for c in chunk])

            try:
                fb_results = graph.post('/', params=params)
            except FacebookClientError:
                try:
                    fb_results = graph.post('/', params=params)
                except FacebookClientError:
                    #fb didn't like this, continue
                    continue

            #iterate over all of the results from facebook
            for fb_result in fb_results:

                if not fb_result.get('body'):
                    print fb_result
                    continue

                parsed_body = json.loads(fb_result['body'])

                if not parsed_body.get('id'):
                    print parsed_body
                    print params
                    continue
                
                if not parsed_body:
                    #SENTRY ERROR LOG HERE
                    #client.captureMessage('Cannot json decode')

                    #sometimes facebook gives us a false body to fuck with us
                    #so lets just skip it

                    '''
                    {
                        'body': 'false',
                        'headers': [{
                            'name': 'Access-Control-Allow-Origin',
                            'value': '*'
                        }, {
                            'name': 'Cache-Control',
                            'value': 'private, no-cache, no-store, must-revalidate'
                        }, {
                            'name': 'Connection',
                            'value': 'close'
                        }, {
                            'name': 'Content-Type',
                            'value': 'text/javascript; charset=UTF-8'
                        }, {
                            'name': 'ETag',
                            'value': '"7cb6efb98ba5972a9b5090dc2e517fe14d12cb04"'
                        }, {
                            'name': 'Expires',
                            'value': 'Sat, 01 Jan 2000 00:00:00 GMT'
                        }, {
                            'name': 'Pragma',
                            'value': 'no-cache'
                        }],
                        'code': 200
                    },
                    '''

                    continue

                #grab our fb db result from item following list from db
                for followed in item['following']:
                    if followed['fb_id'] == parsed_body['id']:
                        #got our match, compare relationship statuses

                        if 'relationship_status' in parsed_body and \
                        parsed_body['relationship_status'] in relationship_codes and \
                        int(relationship_codes[parsed_body['relationship_status']]) != int(followed['rel_status_id']):

                            #got a change!
                            #make and add message to email stories
                            message = parsed_body['name']+' is now '+parsed_body['relationship_status'].lower()
                            email_stories.append({
                                'id': parsed_body['id'],
                                'message': message
                            })
                            #email_stories.append(message);

                            #add DB notification
                            notification = Notification(
                                user_id=item['user_id'], 
                                followed_id=followed['followed_id'],
                                message=message, 
                                rel_status_id=int(relationship_codes[parsed_body['relationship_status']]),
                                timestamp=int(time.time()),
                                fb_id=followed['fb_id']
                            )
                            session.add(notification)

                            #update row
                            session.query(Followed) \
                               .filter_by(id=followed['followed_id']) \
                               .update({Followed.rel_status_id: int(relationship_codes[parsed_body['relationship_status']])})

                        elif 'relationship_status' not in parsed_body and \
                        int(relationship_codes['Not set']) != int(followed['rel_status_id']):

                            #got a change from listed to not listed
                            message = parsed_body['name']+' has hidden their relationship status.'
                            email_stories.append({
                                'id': parsed_body['id'],
                                'message': message
                            })
                            #email_stories.append(message)

                            #add DB notification
                            notification = Notification(
                                user_id=item['user_id'], 
                                followed_id=followed['followed_id'],
                                message=message, 
                                rel_status_id=int(relationship_codes['Not set']),
                                timestamp=int(time.time()),
                                fb_id=followed['fb_id']
                            )
                            session.add(notification)

                            #update row
                            session.query(Followed) \
                               .filter_by(id=followed['followed_id']) \
                               .update({Followed.rel_status_id: int(relationship_codes['Not set'])})

        #send email
        if item['email_opt'] and email_stories:
            subject = today.strftime('People have changed their relationships - SingleYet %m/%d/%Y')

            body = 'You have new SingleYet notifications!\n\n'
            #body = body+'\n'.join(email_stories)

            html_body = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml"><head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/></head><body><div style="background-color: #1B1B1B;color: #fff;font-family: \'Helvetica Neue\',Helvetica,Arial,sans-serif;padding:3px 10px;font-size: 20px;color: #999;font-weight:200;margin-bottom: 15px;"><img src="http://singleyet.com/img/logo_sm.png" width="20"/> SingleYet?</div><strong>People have been changing their relationship statuses</strong><p>Check to see who is <em>single</em> and message them!</p><table width="320" cellspacing="7" style="margin: 10px 0 20px;">'
            for story in email_stories:
                body += '\n'+story['message']
                html_body += '<tr><td width="50"><img src="https://graph.facebook.com/%(id)s/picture"/></td><td width="270"><p style="font-size:13px;color:#333;font-family: \'Helvetica Neue\',Helvetica,Arial,sans-serif;"> %(message)s<br/><a style="text-decoration: none;color:#08C;font-size:13px;font-family: \'Helvetica Neue\',Helvetica,Arial,sans-serif;" href="https://facebook.com/messages/%(id)s" target="_blank">Message</a></p></td></tr>' % {'id': story['id'], 'message': story['message']}

            html_body += '</table><div style="font-size:11px;color:#ccc;border-top:1px solid #ccc;padding-top:5px;">This email was sent to <a style="text-decoration: none;color:#08C;font-size:11px;font-family: \'Helvetica Neue\',Helvetica,Arial,sans-serif;" href="mailto:%(email)s" target="_blank">%(email)s</a>. If you no longer wish to recieve these emails, <a style="text-decoration: none;color:#08C;font-size:11px;font-family: \'Helvetica Neue\',Helvetica,Arial,sans-serif;" href="http://singleyet.com/settings/" target="_blank">unsubscribe</a>.</div></body></html>' % {'email': item['email']}

            pmail = PMMail(api_key=POSTMARK_API_KEY,
                           sender="webmaster@singleyet.com",
                           to=item['email'],
                           subject=subject,
                           text_body=body,
                           html_body=html_body)
            pmail.send()

        queue.task_done()  # Let the queue know that the job is done


if __name__ == '__main__':
    # Start 10 background threads
    for i in xrange(10):
        thread = threading.Thread(target=worker)
        thread.daemon = True
        thread.start()

    main()

    queue.join()  # Wait for all jobs to finish
    session.commit()
    connection.close()
