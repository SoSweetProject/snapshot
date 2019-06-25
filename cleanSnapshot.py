# coding: utf-8

import argparse
import datetime
import logging
import inspect
import ujson
import glob
import os

# Liste des clients à conserver, pour tenter d'exclure les tweets automatiques
goodClients = ["Carbon v2", "Corebird", "Echofon", "Echofon Android", "Echofon  Android", "Echofon for Android PRO", "Fenix for Android", "Flamingo for Android", "Instagram", "Janetter", "MetroTwit", "MeTweets for Windows Phone", "Mobile Web (M2)", "Mobile Web (M5)", "Plume for Android", "Seesmic", "Talon (Classic)", "Talon Android", "Talon (Plus)", "Talon Plus", "TW Blue", "Tweetbot for iΟS", "Tweetbot for Mac", "TweetCaster for Android", "TweetCaster for iOS", "TweetDeck", "Tweetings for Android", "Tweetings for  Windows", "Tweetium for Windows", "twicca", "Twidere for Android #2", "Twidere for Android #3", "Twidere for Android #4", "Twidere for Android #5", "Twidere for Android #7", "Twitter", "Twitter for Android", "Twitter for  Android", "Twitter for Android Tablets", "Twitter for BlackBerry", "Twitter for BlackBerry®", "Twitter for iPad", "Twitter for iPhone", "Twitter for  iPhone", "Twitter for Mac", "Twitter for Samsung Tablets", "Twitter for Windows", "Twitter for Windows Phone", "Twitter Lite", "Twitter Web Client", "Twitterrific", "Twitterrific for Mac", "Twitterrific for iOS", "Twittnuker", "Wxbooks", "YoruFukurou"]

def parseArgs():
    parser = argparse.ArgumentParser(description='fusionne les tweets des deux périodes de collecte (gnip/datasift et tweepy) au format du snapshot, et ne conserve que les tweets en français et provenant de clients spécifiques')
    parser.add_argument('--path_to_gnipDatasif_data', '-g', required=True, help='répertoire des tweets collectés avec Gnip ou Datasift')
    parser.add_argument('--path_to_tweepy_data', '-i', required=True, help='répertoire des tweets collectés avec Tweepy')
    parser.add_argument('--path_to_output_data', '-o', required=True, help='répértoire des fichiers fusionnés et triés')
    parser.add_argument('--log-file-path', '-P', default='.', help='log file path')
    parser.add_argument('--from-date', '-f', required=False, default="2006-01-01", help='Date of begining of the clean snapshot. Format: 2014-01-31')
    parser.add_argument('--to-date', '-t', required=True, default=None, help='Date of the end of the clean snapshot. Format: 2014-09-01')
    args = parser.parse_args()
    if args.path_to_gnipDatasif_data == args.path_to_output_data or args.path_to_tweepy_data == args.path_to_output_data :
        raise ValueError(
            "paths to input data files and output data files must be different")
    return args

def getFilesToTreat(path, from_date, to_date):
    files = [f for f in glob.glob(path + '*.data') if (f.split('/')[-1].split('T')[0] <= to_date and f.split('/')[-1].split('T')[0] >= from_date)]
    files.sort()
    logger.info("%d files to treat"%len(files))
    return files

args = parseArgs()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(args.log_file_path+'/'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)

if args.path_to_gnipDatasif_data[-1] != '/':
    args.path_to_gnipDatasif_data += '/'
if args.path_to_tweepy_data[-1] != '/':
    args.path_to_tweepy_data += '/'
if args.path_to_output_data[-1] != '/':
    args.path_to_output_data += '/'
logger.info("input directory (GnipDatasift) : %s"%args.path_to_gnipDatasif_data)
logger.info("input directory (Tweepy) : %s"%args.path_to_tweepy_data)
logger.info("output directory: %s"%args.path_to_output_data)

tweepyFiles = getFilesToTreat(args.path_to_tweepy_data, args.from_date, args.to_date)
tweepyPath = os.listdir(args.path_to_tweepy_data)

# On se base sur les fichiers de tweets collectés avec Tweepy, car ils couvrent toute la période des tweets collectés avec Gnip ou Datasift
for i,file in enumerate(tweepyFiles) :
    logger.info("traitement du fichier %s (%d/%d)"%(os.path.basename(file),i+1,len(tweepyFiles)))

    # dictionnaire qui contiendra l'ensemble des tweets triés (avec l'id du tweet en clé pour éviter les doublons)
    mergedTweets = {}

    tweepyFile = open(file, "r")

    try :
        gnuDatasiftFile = open(args.path_to_gnipDatasif_data+os.path.basename(file), "r")

        # On trie d'abord les tweets collectés lors de la première période de collecte
        # Le tri s'effectue en fonction de la langue du tweet detectée par Twitter (on ne conserve que les tweets en français), ainsi qu'en fonction des clients Twitter (afin d'éviter de conserver les tweets automatiques)
        for gnuDatasiftLine in gnuDatasiftFile :
            gnuDatasiftTweet = ujson.loads(gnuDatasiftLine)
            # Par précaution on vérifie que le champ twitter est bien dans "language" car certains tweets ne l'ont pas (en 2016-08-09, 2016-08-10 et  dans quelques fichiers de 2016-05), mais cela ne concerne que de très rares cas
            if "twitter" in gnuDatasiftTweet["language"] :
                if (gnuDatasiftTweet["client"] in goodClients and gnuDatasiftTweet["language"]["twitter"]=="fr") :
                    mergedTweets[gnuDatasiftTweet["id"]]=gnuDatasiftTweet

        gnuDatasiftFile.close()

    except :
        logger.info("le fichier %s n'existe pas dans %s"%(os.path.basename(file),args.path_to_gnipDatasif_data))

    # Puis ceux collectés lors de la deuxième période de collecte
    for tweepyLine in tweepyFile :
        tweepyTweet = ujson.loads(tweepyLine)
        if (tweepyTweet["id"] not in mergedTweets and tweepyTweet["client"] in goodClients and tweepyTweet["language"]["twitter"]=="fr") :
            mergedTweets[tweepyTweet["id"]]=tweepyTweet

    # Écriture des tweets triés et dédoublonnés dans le fichier de sortie
    if (len(mergedTweets)!=0) :
        mergedFile = open(args.path_to_output_data+os.path.basename(file), "w")

        for tweet in mergedTweets :
            tweet=ujson.dumps(mergedTweets[tweet])
            mergedFile.write(tweet+"\n")

        mergedFile.close()

    tweepyFile.close()
