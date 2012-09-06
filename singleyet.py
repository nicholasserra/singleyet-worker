#!/usr/bin/env python
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from facebook import GraphAPI
import simplejson as json

from postmark import PMMail

POSTMARK_API_KEY = '***REMOVED***'

import time

Base = declarative_base()
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

    #connect to db
    engine = create_engine('mysql://singleyet:***REMOVED***@localhost/singleyet')
    connection = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    #grab and sort relationship codes
    rel_result = connection.execute("SELECT * FROM `rel_status`")
    relationship_codes = {}

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
        email_stories = []

        params = {'access_token': item['access_token']}
        params['batch'] = json.dumps([{'method': 'GET', 'relative_url': '/'+following['fb_id']} for following in item['following']])

        graph = GraphAPI(item['access_token'])
        fb_results = graph.post('/', params=params)

        #iterate over all of the results from facebook
        for fb_result in fb_results:
            parsed_body = json.loads(fb_result['body'])

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
                        email_stories.append(message);

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
                        followed_row = session.query(Followed) \
                                       .filter_by(id=followed['followed_id']) \
                                       .update({Followed.rel_status_id: int(relationship_codes[parsed_body['relationship_status']])})

                    elif 'relationship_status' not in parsed_body and \
                    int(relationship_codes['Not set']) != int(followed['rel_status_id']):

                        #got a change from listed to not listed                        
                        message = parsed_body['name']+' has hidden their relationship status.'
                        email_stories.append(message)

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
                        followed_row = session.query(Followed) \
                                       .filter_by(id=followed['followed_id']) \
                                       .update({Followed.rel_status_id: int(relationship_codes['Not set'])})

        #send email
        if item['email_opt'] and email_stories:
            pmail = PMMail(api_key=POSTMARK_API_KEY,
                           sender="webmaster@singleyet.com",
                           to=item['email'],
                           subject='You have new SingleYet notifications',
                           text_body='\n'.join(email_stories))
            pmail.send()

    session.commit()
    connection.close()

if __name__ == '__main__':
    main()