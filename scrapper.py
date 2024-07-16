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
    df = pd.DataFrame(columns=['User_name', 'tweets','Original tweets', 'replies', 'friendship'])
    api = API()

    for cred in credentials:
        await api.pool.add_account(cred[0], cred[1], cred[2], cred[3])
    await api.pool.login_all()

    for user in users_list:  
        user_info = await api.user_by_login(user)
        if user_info is None:
            continue

        user_id = user_info.dict()['id']
        print(f"Scraping data for user: {user}, ID: {user_id}")

        tweets = await gather(api.user_tweets(user_id, limit=text_limit))
        retweets = await gather(api.user_tweets_and_replies(user_id, limit=text_limit))
        followings = await gather(api.following(user_id, limit=text_limit))
        followers = await gather(api.followers(user_id, limit=text_limit))
        mutuality = []
        if tweets:
            followers_ids = [follower.dict()['id'] for follower in followers]
            following_ids = [following.dict()['id'] for following in followings]
            tts = [tweet.dict()['rawContent'] for tweet in tweets]
            replies = [reply.dict()['rawContent'] for reply in retweets if reply.dict().get('retweetedTweet') is None]
            rettwt = [retweet.dict()['retweetedTweet']['rawContent'] if retweet.dict().get('retweetedTweet') else None for retweet in retweets]
            for retweet in retweets:
                if retweet.dict().get('retweetedTweet') is None:
                    mutuality.append('Not Mutual')
                elif retweet.dict()['retweetedTweet']['user']['id'] in followers_ids and user_id in following_ids:
                    mutuality.append('Mutual')
                else:
                    mutuality.append('Not Mutual')
            chunk = [{'User_name': tweets[0].dict()['user']['username'], 'tweets': tts, 'Original tweets': replies, 'replies': rettwt, 'friendship': mutuality}]
            df = pd.concat([df, pd.DataFrame(chunk)])
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
