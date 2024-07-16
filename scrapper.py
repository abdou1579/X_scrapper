import argparse
import re
import pandas as pd
from twscrape import API, gather
import asyncio

async def scrap_tweets(credentials_file, users_path, text_limit):
    with open(credentials_file, 'r') as file:
        credentials = []
        for line in file:
            parts = line.strip().split(':')
            credentials.append(parts)

    with open(users_path, 'r') as file:
        data = file.read()
    match = re.search(r'users\s*=\s*(\[.*\])', data, re.DOTALL)
    if match:
        users_list_str = match.group(1)
        users_list = eval(users_list_str)
    else:
    	users_list = []

    df = pd.DataFrame(columns=['tweet_id', 'user','created_at', 'post_text','lang','ViewCount','quoteCount','likeCount','replyCount','retweetCount'
,'bookmarkCount' ,'is_retweet','is_quote','reply_to_id','reply_to_user','Original tweets', 'reply_to_text', 'is_mutual_followership'])
    api = API()
    await api.pool.add_account(credentials[0][0], credentials[0][1], credentials[0][2], credentials[0][3])
    await api.pool.login_all()
    for user in users_list[:30]:
        if await api.user_by_login(user) == None:
            pass
        else:
            user_id = (await api.user_by_login(user)).dict()['id']
            tweets = await gather(api.user_tweets(user_id, limit=text_limit))
            retweets = await gather(api.user_tweets_and_replies(user_id, limit=text_limit))
            followings = await gather(api.following(user_id, limit=text_limit))
            followers = await gather(api.followers(user_id, limit=text_limit))
            username = await api.user_by_id(user_id)
            mutuality = []
            date = []
            quote_count= []
            like_count = []
            reply_count =[]
            retweet_count = []
            bookmark_count = []
            viewcount = []
            lang=[]
            tweet_ids = []
            is_quote = []
            is_retweet = []
            replied_to_id = []
            replied_to_username = []
            if len(tweets) != 0 :
                followers_ids = []
                following_ids = []
                for k in range(len(followings)):
                    following_ids.append(followings[k].dict()['id'])
                for l in range(len(followers)):
                    followers_ids.append(followers[l].dict()['id'])
                tts = []
                replies = []
                rettwt = []
                ##for tweet_id we need to check if the tweet was quoted or retweeted or a normal tweet
                #for i in range(len(tweets)):
                    #if tweets[i].dict()['
                for i in range(len(tweets)):
                    tts.append([tweets[i].dict()['rawContent']])
                    if tweets[i].dict()['user']['username'] == username.dict()['username']:
                        tweet_ids.append([tweets[i].dict()['id']])
                        date.append([tweets[i].dict()['date'].strftime("%Y/%m/%d %H:%M:%S")])
                        lang.append([tweets[i].dict()['lang']])
                        viewcount.append([tweets[i].dict()['viewCount']])
                        quote_count.append([tweets[i].dict()['quoteCount']])
                        like_count.append([tweets[i].dict()['likeCount']])
                        reply_count.append([tweets[i].dict()['replyCount']])
                        retweet_count.append([tweets[i].dict()['retweetCount']])
                        bookmark_count.append([tweets[i].dict()['bookmarkedCount']])
                        if tweets[i].dict()['retweetedTweet'] != None:
                            is_retweet.append([True])
                            replied_to_id.append([tweets[i].dict()['retweetedTweet']['user']['id']])
                            replied_to_username.append([tweets[i].dict()['retweetedTweet']['user']['username']])
                        else:is_retweet.append([False])
                        if tweets[i].dict()['quotedTweet'] != None:
                            replied_to_id.append([tweets[i].dict()['quotedTweet']['user']['id']])
                            replied_to_username.append([tweets[i].dict()['quotedTweet']['user']['username']])                            
                            is_quote.append([True])
                        else:is_quote.append([False])
                            
                for j in range(len(retweets)):
                    if retweets[j].dict()['user']['username'] != username.dict()['username']:
                        replies.append([retweets[j].dict()['rawContent']])
                    else:
                        rettwt.append([retweets[j].dict()['rawContent']])
                    if retweets[0].dict()['retweetedTweet'] == None:
                        mutuality.append('Not Mutual')
                    else:
                        if retweets[0].dict()['retweetedTweet']['user']['id'] in followers_ids and user_id in following_ids:
                            mutuality.append('Mutual')
                        else:
                            mutuality.append('Not Mutual')
                        
                chunk = []
                chunk.append({'tweet_id':tweet_ids, 'user': tweets[0].dict()['user']['username'],'created_at':date, 'post_text':tts,'lang':lang,'ViewCount':viewcount,'quoteCount':quote_count,
'likeCount': like_count, 'replyCount' : reply_count, 'retweetCount' : retweet_count, 'bookmarkCount' : bookmark_count,'is_retweet':is_retweet, 'is_quote':is_quote,'reply_to_id':replied_to_id,
'reply_to_user':replied_to_username, 'Original tweets':replies, 'reply_to_text':rettwt, 'is_mutual_followership':mutuality})
                df = pd.concat([df, pd.DataFrame(chunk)])
            else: pass
    return df

async def main(credentials_file, users_path, text_limit, path_to_save):
    df = await scrap_tweets(credentials_file, users_path, int(text_limit))
    df.to_csv(path_to_save, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('credentials', metavar='FILE', type=str, help='Path to the credentials text file')
    parser.add_argument('users_path', metavar='FILE', type=str, help='Path to the users list file')
    parser.add_argument('text_limit', metavar='LIMIT', type=int, help='Text limit')
    parser.add_argument('path_to_save', metavar='FILE', type=str, help='Path to save the generated scraping file')

    args = parser.parse_args()

    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args.credentials, args.users_path, args.text_limit, args.path_to_save))
