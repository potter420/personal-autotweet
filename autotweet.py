import multiprocessing as mp
import sqlite3
import time
from twython import TwythonStreamer, Twython
import math, os
import logging
import requests

creds = {}
creds['CONSUMER_KEY'] = os.environ['TWITTER_CONSUMER_KEY']
creds['CONSUMER_SECRET'] = os.environ['TWITTER_CONSUMER_SECRET']
creds['ACCESS_TOKEN'] = os.environ['TWITTER_ACCESS_TOKEN']
creds['ACCESS_SECRET'] = os.environ['TWITTER_ACCESS_SECRET']
database = './tweet.db'
logdir = './AutoTweetLog/'
tablename = 'most_recent_tweet'
retweetedid = 'retweeted'

class MyStreamer(TwythonStreamer):     
    
    def __init__(self, *arg, uid_list = '', tweetdb = None, callback = None, **kwarg,):
        super().__init__( *arg, **kwarg)
        self.uidl = uid_list.split(',')[:-1]
        self.me = uid_list.split(',')[-1]
        self.db = tweetdb
        self.callback = callback      
        
    # Received data
    def on_success(self, data):
        if 'id' in data.keys():
            self.insert_data(data)
    
    def insert_data(self, tweet):
        query = '''insert into most_recent_tweet
                    (id, is_retweeted, is_inlist, is_replyme, is_retweeted_me, is_retweeted_list, retweeted_id,
                    retweet_has_media, is_favorited, has_media, has_mentioned) 
                    values ('{id}',{is_retweeted},{is_inlist},{is_replyme},{is_retweeted_me},{is_retweeted_list},'{retweeted_id}',
                      {retweet_has_media},{is_favorited},{has_media},{has_mentioned});'''\
            .format(id = tweet['id'], 
                    is_retweeted = 1 if 'retweeted_status' in tweet.keys() else 0,
                    is_inlist = 1 if tweet['user']['id_str'] in self.uidl else 0,
                    is_replyme = 1 if tweet['in_reply_to_user_id'] == self.me else 0,
                    is_retweeted_me = 1 if tweet.get('retweeted_status',{'user':{'id_str':None}})['user']['id_str'] == self.me else 0,
                    is_retweeted_list = 1 if tweet.get('retweeted_status',{'user':{'id_str':None}})['user']['id_str'] in self.uidl else 0,
                    retweeted_id = tweet.get('retweeted_status',{'id_str':None})['id_str'] or '',
                    retweet_has_media = 1 if 'media' in tweet.get('retweeted_status',{'entities':{}}).get('entities',{}).keys() else 0,
                    is_favorited = 1 if tweet['favorited'] else 0,
                    has_media = 1 if 'media' in tweet.get('entities',{}).keys() else 0,
                    has_mentioned = 1 if len(tweet['entities']['user_mentions'])>0 else 0
                   )
        self.callback(query)
        
    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.callback('Terminate')
        self.disconnect()

def create_logger(app_name, level = 'DEBUG', logdir = None):
    mapping = {'DEBUG':logging.DEBUG, 
                'INFO':logging.INFO}
    logger = logging.getLogger(app_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(mapping[level])
    fh = logging.FileHandler(os.path.join(logdir, app_name + '.log'))
    fh.setLevel(mapping[level])
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger



def init_db(constr):
    try:
        os.remove(constr)
    except:
        pass
    with sqlite3.connect(constr) as conn:
        conn.execute('drop table if exists {0};'.format(tablename))
        conn.execute('''create table {0} (
                 id text,
                 is_retweeted integer,
                 is_inlist integer,
                 is_replyme integer,
                 is_retweeted_me integer,
                 is_retweeted_list integer,
                 retweeted_id text,
                 retweet_has_media integer,
                 is_favorited integer,
                 has_media integer,
                 has_mentioned integer
            );
            '''.format(tablename))
        conn.execute('drop table if exists {0}_staging;'.format(tablename))
        conn.execute('create table {0}_staging as select * from {0};'.format(tablename))
        conn.execute('create table {0} (id text)'.format(retweetedid))
        conn.commit()
        
def listen_for_tweet(q):
    with open('/home/thanhtm1/userlist.txt', mode='r') as f:
        comma_separated_string = f.read()
    twitter = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], 
                        creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])
    output = twitter.lookup_user(screen_name=comma_separated_string)
    uidl=','.join(user["id_str"] for user in output) + ', 3161243935'
    def insertq(query):
        if query != 'Terminate':
            q.put([query , 0])
        else:
            q.put(['',9])
    stream = MyStreamer(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], 
                        creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'],
                       uid_list = uidl, callback = insertq)
    while True:
        try:
            stream.statuses.filter(follow=uidl)
        except requests.exceptions.ChunkedEncodingError:
            pass
        except:
            break
           
def write_to_db(q, retq, constr):
    logger =  create_logger('write_to_db', logdir = logdir)
    while True:
        d = q.get()
        if d[1]==9:
            break
        #logger.debug('Execute: %s'%(d.__repr__()).replace('\n',' '))
        with sqlite3.connect(constr) as conn:
            if d[1]==1:
                conn.execute('delete from {0}_staging;'.format(tablename))
                conn.execute('insert into {0}_staging select * from {0};'.format(tablename))
                conn.execute('delete from {0};'.format(tablename))
                retq.put('Done')
            elif d[1]==0:
                conn.execute(d[0])
            conn.commit()
            
def batch_processing(q, retq, constr):
    pred = 7.5
    logger =  create_logger('batch_processing', logdir = logdir)
    with sqlite3.connect(constr) as conn:
        while True:
            time.sleep(pred*60)
            n = math.floor(1000/24/(60/pred))
            q.put(['most_recent_tweet',1])
            resp = retq.get()
            if resp == 'Quit':
                break
            else:
                #Retweet
                twitter = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])
                rss = conn.execute('''select retweeted_id 
                        from {0}_staging 
                        where is_retweeted_list = 1 
                            and is_inlist=1 
                            and retweet_has_media=1;'''.format(tablename)).fetchall()
                for e in rss:
                    try:
                        twitter.retweet(id = e[0])
                    except:
                        logger.debug('Error retweeting Tweet: %s'%e.__repr__())
                logger.debug('Retweeting: %s'%rss.__repr__())
                #Like
                rss = conn.execute('''select id from {0}_staging
                         where is_favorited = 0 
                         order by is_retweeted_me desc, is_replyme desc limit {1}
                         '''.format(tablename, n)).fetchall()
                for e in rss:
                    try:
                        twitter.create_favorite(id = e[0])
                    except:
                        logger.debug('Error liking Tweet: %s'%e.__repr__())
                logger.debug('Favouriting: %s'%rss.__repr__())

if __name__ == '__main__':
    init_db(database)
    logger = create_logger('main', logdir = logdir)
    logger.info('Start AutoTweet')
    q = mp.Queue()
    retq = mp.Queue()
    ls1 = mp.Process(target=listen_for_tweet, args=(q,))
    ls2 = mp.Process(target=write_to_db, args=(q, retq, database))
    ls3 = mp.Process(target=batch_processing, args=(q,retq, database))
    ls1.start()
    ls2.start()
    ls3.start()
    ls1.join()
    q.put(['None',9])
    ls2.join()
    retq.put('Quit')
    ls3.join()
    logger.info('Exiting')
