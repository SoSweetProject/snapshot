import argparse
import logging
import datetime
import tarfile
import inspect
import ujson
import time
import glob
import sys
import ast
import re

logger = logging.getLogger(__name__)

def parseArgs():
    parser = argparse.ArgumentParser(description='Create a set of files containg the text of each tweet, its user and its date.')
    parser.add_argument('--path_to_input_data', '-p', required=True, help='path to input data files')
    parser.add_argument('--path_to_output_data', '-o', required=True, help='path to output data files')
    parser.add_argument('--log-level', '-l', default='info',help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', default='file', help='logger destination', choices=['file', 'stderr'])
    parser.add_argument('--log-file-path', '-P', default='.', help='log file path')
    parser.add_argument('--from-date', '-f', required=False, default="2014-01-01", help='Date of begining of the snapshot. Format: 2014-09-01')
    parser.add_argument('--to-date', '-t', required=True, default=None, help='Date of the end of the snapshot. Format: 2014-09-01')
    parser.add_argument('--from_tweepy', '-T', required=True, help='True if the tweets were retrieved with Tweepy, False if not', choices=["True", "False"])
    args = parser.parse_args()
    if args.path_to_input_data == args.path_to_output_data:
        raise ValueError(
            "paths to input data files and output data files must be different")
    return args


def getFilesToTreat(path, from_date, to_date):
    logger.info("looking for uncompressed files to treat")
    #first, uncompressed files
    uncompressedFiles = [f for f in glob.glob(path + '*.data') if (f.split('/')[-1].split('T')[0] <= to_date and f.split('/')[-1].split('T')[0] >= from_date)]
    logger.debug("data files:")
    logger.debug("\n".join(uncompressedFiles))
    uncompressedFiles.sort()
    #then compressed files
    logger.info("looking for compressed files to treat")
    compressedFiles=[]
    for fileName in [f for f in glob.glob(path + '*.tgz') if (f.split('/')[-1].split('.')[0] >= from_date and f.split('/')[-1].split('.')[0] <= to_date)]:
        logger.info("checking:%s"%fileName)
        tf=tarfile.open(fileName)
        filesOK=[(n,tf) for n in tf.getnames() if n.split('/')[-1].split('T')[0] >= from_date and n.split('/')[-1].split('T')[0] <= to_date and n.split('.')[-1] == "data" and n.split('.')[-2] != "retweets"]
        logger.debug("\n".join([f[0] for f in filesOK]))
        compressedFiles+=filesOK
    compressedFiles.sort()
    files=compressedFiles+uncompressedFiles
    logger.info("%d files to treat"%len(files))
    # logger.debug("\n".join([f if files))
    return files


def treatFile(input_file_name, path_to_output_data, from_tweepy):
    try:
        t0=datetime.datetime.now()
        if type(input_file_name) == str:#a non compressed file
            logger.debug("treating %s"%input_file_name)
            fileIn = open(input_file_name)
            output_file_name=path_to_output_data + input_file_name.split('/')[-1]
            fileOut = open(output_file_name, "w")
        else :# a data file in a tgz file
            logger.debug("treating %s"%input_file_name[0])
            logger.debug("extracting %s"%input_file_name[0])
            fileIn = input_file_name[1].extractfile(input_file_name[0])
            output_file_name=path_to_output_data + input_file_name[0].split('/')[-1]
            fileOut = open(output_file_name, "w")
        logger.debug("output file: %s"%output_file_name)
        isDatasift = isDatasiftFile(input_file_name)
        i=0
        for i,l in enumerate(fileIn):
            logger.debug("treating line %d"%i)
            if from_tweepy :
                tweet=treatTweepyTweets(l)
            elif isDatasift:
                tweet=treatDatasiftTweet(l)
            else:
                tweet=treatGnipTweet(l)
            if not tweet:
                logger.debug("failed to treat line %d"%i)
                continue
            fileOut.write(ujson.dumps(tweet) + "\n")
        fileOut.close()
        t1=datetime.datetime.now()
        logger.info("file treated in %.2f seconds at %.2f lines per second"%((t1-t0).total_seconds(),i/(t1-t0).total_seconds() if (t1-t0).total_seconds()!=0 else 0))
    except Exception as e:
        logger.exception("failed to treat file!")

def isDatasiftFile(fileName):
    name = fileName if type(fileName)==str else fileName[0]
    logger.debug("checking whether %s is from datasift"%name)
    logger.debug(name.split('/')[-1].split('T')[0])
    logger.debug('this is a ' + ('datasift' if name.split('/')[-1].split('T')[0] < '2016' else 'gnip')+' file')
    return name.split('/')[-1].split('T')[0] < '2016'

def treatTweepyTweets(line) :
    try:
        tweet = ujson.loads(line)
    except ValueError:
        logger.warning("cannot parse line: %s"%line)
        return None
    if (tweet["source"]!="") :
        client = re.match(r"<a.+?>(.+?)</a>",tweet["source"]).group(1)
    else :
        client = ""
    date = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
    condensate = {
        "tweet": tweet['full_text'],
        'date': date,
        'user': {
                'id':tweet['user']['id_str'],
                'timezone': tweet['user']['time_zone']
                },
        'id':tweet['id_str'],
        'client':client,
        'collectedWith':'tweepy'}
    if tweet["geo"] is not None :
        condensate['geo']={}
        condensate['geo']['longitude']=tweet["geo"]['coordinates'][1]
        condensate['geo']['latitude']=tweet["geo"]['coordinates'][0]
    condensate['language']={}
    condensate['language']['twitter']=tweet['lang']
    condensate['language']['user']=tweet['user']['lang']
    if (tweet['in_reply_to_status_id'] is not None or tweet['in_reply_to_user_id'] is not None) :
        condensate['in_reply_to']={}
        condensate['in_reply_to']['tweet_id']= tweet['in_reply_to_status_id']
        condensate['in_reply_to']['user_id']= tweet['in_reply_to_user_id']
    if ("user_mentions" in tweet['entities'] and len(tweet['entities']["user_mentions"])>0) :
        condensate['mentions']=[h['id_str'] for h in tweet['entities']['user_mentions']]
    if ("hashtags" in tweet['entities'] and len(tweet['entities']["hashtags"])>0) :
        condensate['hashtags']=[h['text'] for h in tweet['entities']['hashtags']]
    if ("urls" in tweet['entities'] and len(tweet['entities']["urls"])>0) :
        condensate['urls']=[h['expanded_url'] for h in tweet['entities']['urls']]
    if ("media" in tweet['entities'] and len(tweet["entities"]["media"])>0) :
        condensate['media']=[h['media_url'] for h in tweet['entities']['media']]
    return condensate

def treatDatasiftTweet(line):
    try:
        tweet = ujson.loads(line)
    except ValueError:
        logger.warning("cannot parse line: %s"%line)
        return None
    if (re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",tweet['twitter']['created_at'])) :
        date = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.strptime(tweet['twitter']['created_at'],r'%Y-%m-%dT%H:%M:%S'))
    else :
        date = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.strptime(tweet['twitter']['created_at'],r'%Y-%m-%dT%H:%M:%S+00:00'))
    condensate = {
        "tweet": tweet['twitter']['text'],
        'date': date,
        'user': {
                'id':tweet['twitter']['user']['id'],
                'timezone': tweet['twitter']['user']['time_zone']
                },
        'id':tweet['twitter']['id'],
        'client':tweet['interaction']['source'],
        'collectedWith':'datasift'}
    if "geo" in tweet['twitter']:
        condensate['geo']={}
        condensate['geo']['latitude']=tweet['twitter']["geo"]['latitude']
        condensate['geo']['longitude']=tweet['twitter']["geo"]['longitude']
    condensate['language']={}
    condensate['language']['user']=tweet['twitter']['user']['lang']
    condensate['language']['twitter']=tweet['twitter']['lang']
    if 'language' in tweet:
        condensate['language']['datasift']={}
        condensate['language']['datasift']['language']=tweet['language']['tag']
        condensate['language']['datasift']['confidence']=tweet['language']['confidence']
    if 'in_reply_to_status_id' in tweet['twitter']:
        condensate['in_reply_to']={}
        condensate['in_reply_to']['tweet_id']= tweet['twitter']['in_reply_to_status_id'],
        if 'in_reply_to_user_id' in tweet['twitter']:
            condensate['in_reply_to']['user_id']= tweet['twitter']['in_reply_to_user_id'],
    if 'mentions' in tweet['twitter']:
        condensate['mentions']=tweet['twitter']['mentions']
    if 'hashtags' in tweet['twitter']:
        condensate['hashtags']=tweet['twitter']['hashtags']
    if 'links' in tweet['twitter']:
        condensate['urls']=tweet['twitter']['links']
    if 'media' in tweet['twitter']:
        condensate['media']=[m['expanded_url'] for m in tweet['twitter']['media']]
    return condensate

def treatGnipTweet(line):
    try:
        tweet = ujson.loads(line)
    except ValueError:
        logger.warning("cannot parse line: %s"%line)
        return None
    condensate = {
        'date': tweet['postedTime'],
        'user': {
                'id':tweet['actor']['id'].split(':')[-1],
                'timezone':tweet['actor']['twitterTimeZone']
        },
        'id':tweet['id'].split(':')[-1],
        'client':tweet["generator"]['displayName'],
        'collectedWith':'gnip'}
    if "long_object" in tweet:
        condensate["tweet"]=tweet['long_object']['body']
        if len(tweet['long_object']['twitter_entities']['hashtags'])>0:
            condensate['hashtags']=[h['text'] for h in tweet['long_object']['twitter_entities']['hashtags']]
        if len(tweet['long_object']['twitter_entities']['urls'])>0:
            condensate['urls']=[h['expanded_url'] for h in tweet['long_object']['twitter_entities']['urls']]
        if len(tweet['long_object']['twitter_entities']['user_mentions'])>0:
            condensate['mentions']=[h['id_str'] for h in tweet['long_object']['twitter_entities']['user_mentions']]
        if 'media' in tweet['long_object']['twitter_entities'] and len(tweet['long_object']['twitter_entities']['media'])>0:
            condensate['media']=[h['media_url'] for h in tweet['long_object']['twitter_entities']['media']]
    else:
        condensate["tweet"]= tweet['body']
        if len(tweet['twitter_entities']['hashtags'])>0:
            condensate['hashtags']=[h['text'] for h in tweet['twitter_entities']['hashtags']]
        if len(tweet['twitter_entities']['urls'])>0:
            condensate['urls']=[h['expanded_url'] for h in tweet['twitter_entities']['urls']]
        if len(tweet['twitter_entities']['user_mentions'])>0:
            condensate['mentions']=[h['id_str'] for h in tweet['twitter_entities']['user_mentions']]
        if 'media' in tweet['twitter_entities'] and len(tweet['twitter_entities']['media'])>0:
            condensate['media']=[h['media_url'] for h in tweet['twitter_entities']['media']]
    if "geo" in tweet:
        condensate['geo']={}
        condensate['geo']['latitude']=tweet['geo']['coordinates'][0]
        condensate['geo']['longitude']=tweet['geo']['coordinates'][1]
    condensate['language']={}
    if 'languages' in tweet['actor'] and len(tweet['actor']['languages'])>0:
        condensate['language']['user']=tweet['actor']['languages'][0]
    if 'twitter_lang' in tweet:
        condensate['language']['twitter']=tweet['twitter_lang']
    if 'language' in tweet['gnip']:
        condensate['language']['gnip']=tweet['gnip']['language']['value']
    if 'inReplyTo' in tweet:
            condensate['in_reply_to']={}
            condensate['in_reply_to']['tweet_id']= tweet['inReplyTo']['link'].split('/')[-1]
            condensate['in_reply_to']['user_id']=None
    return condensate

def main():
    args = parseArgs()

    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        handler = logging.FileHandler(args.log_file_path+'/'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    if args.path_to_input_data[-1] != '/':
        args.path_to_input_data += '/'
    if args.path_to_output_data[-1] != '/':
        args.path_to_output_data += '/'
    logger.info("input directory: %s"%args.path_to_input_data)
    logger.info("output directory: %s"%args.path_to_output_data)

    files=getFilesToTreat(args.path_to_input_data, args.from_date, args.to_date)

    from_tweepy = ast.literal_eval(args.from_tweepy)

    for i,file in enumerate(files):
        if i%10==0:
            logger.info("%d/%d files treated"%(i,len(files)))
        logger.info('treating ' + file if type(file)==str else file[0])
        treatFile(file, args.path_to_output_data, from_tweepy)

if __name__ == '__main__':
    main()
else:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)
