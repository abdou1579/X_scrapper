import argparse
import re
import pandas as pd
from twscrape import API, gather
import asyncio
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrap_tweets(credentials_file, users_path, text_limit):
    logging.info("Starting the scraping process")

    with open(credentials_file, 'r') as file:
        credentials = [line.strip().split(':') for line in file]

    with open(users_path, 'r') as file:
        data = file.read()
    match = re.search(r'users\s*=\s*(\[.*\])', data, re.DOTALL)
    users_list = eval(match.group(1)) if match else []

    df = pd.DataFrame(columns=[
        'tweet_id', 'user', 'created_at', 'post_text', 'lang', 'ViewCount',
        'quoteCount', 'likeCount', 'replyCount', 'retweetCount', 'bookmarkCount',
        'is_retweet', 'is_quote', 'reply_to_id', 'reply_to_user', 'reply_to_text', 'is_mutual_followership'
    ])

    api = API()

    async def add_account_if_not_exists(account):
        try:
            await api.pool.add_account(account[0], account[1], account[2], account[3])
            logging.info(f"Added account {account[0]}")
        except Exception as e:
            logging.warning(f"Warning: {e}")

    logging.info("Adding accounts")
    for credential in credentials:
        await add_account_if_not_exists(credential)

    logging.info("Logging in all accounts")
    await api.pool.login_all()

    logging.info("Starting to scrape users")
    for user in users_list[:30]:
        logging.info(f"Scraping tweets for user: {user}")
        user_info = await api.user_by_login(user)
        if user_info is None:
            logging.warning(f"User {user} not found")
            continue
        user_id = user_info.dict()['id']

        try:
            tweets = await gather(api.user_tweets(user_id, limit=text_limit))
            retweets_and_replies = await gather(api.user_tweets_and_replies(user_id, limit=text_limit))
        except Exception as e:
            logging.warning(f"Rate limit hit, waiting before retrying: {e}")
            time.sleep(900)  # wait for 15 minutes
            tweets = await gather(api.user_tweets(user_id, limit=text_limit))
            retweets_and_replies = await gather(api.user_tweets_and_replies(user_id, limit=text_limit))

        followings = await gather(api.following(user_id, limit=text_limit))
        followers = await gather(api.followers(user_id, limit=text_limit))

        followers_ids = {follower.dict()['id'] for follower in followers}
        following_ids = {following.dict()['id'] for following in followings}

        for tweet in tweets + retweets_and_replies:
            tweet_data = tweet.dict()
            mutuality = 'Mutual' if tweet_data['user']['id'] in followers_ids and user_id in following_ids else 'Not Mutual'

            is_retweet = tweet_data['retweetedTweet'] is not None
            is_quote = tweet_data['quotedTweet'] is not None
            is_reply = tweet_data.get('inReplyToStatus') is not None

            if is_reply:
                original_tweet_id = tweet_data.get('inReplyToStatus')
                original_tweet = await api.tweet(original_tweet_id)
                original_tweet_data = original_tweet.dict() if original_tweet else {}
                reply_to_id = original_tweet_data.get('id')
                reply_to_user = original_tweet_data.get('user', {}).get('username')
                reply_to_text = original_tweet_data.get('rawContent')
            else:
                reply_to_id = tweet_data.get('retweetedTweet', {}).get('user', {}).get('id') if is_retweet else (tweet_data.get('quotedTweet', {}).get('user', {}).get('id') if is_quote else None)
                reply_to_user = tweet_data.get('retweetedTweet', {}).get('user', {}).get('username') if is_retweet else (tweet_data.get('quotedTweet', {}).get('user', {}).get('username') if is_quote else None)
                reply_to_text = tweet_data.get('retweetedTweet', {}).get('rawContent') if is_retweet else (tweet_data.get('quotedTweet', {}).get('rawContent') if is_quote else None)

            new_row = pd.DataFrame([{
                'tweet_id': tweet_data['id'],
                'user': tweet_data['user']['username'],
                'created_at': tweet_data['date'].strftime("%Y/%m/%d %H:%M:%S"),
                'post_text': tweet_data['rawContent'],
                'lang': tweet_data['lang'],
                'ViewCount': tweet_data['viewCount'],
                'quoteCount': tweet_data['quoteCount'],
                'likeCount': tweet_data['likeCount'],
                'replyCount': tweet_data['replyCount'],
                'retweetCount': tweet_data['retweetCount'],
                'bookmarkCount': tweet_data['bookmarkedCount'],
                'is_retweet': is_retweet,
                'is_quote': is_quote,
                'reply_to_id': reply_to_id,
                'reply_to_user': reply_to_user,
                'reply_to_text': reply_to_text,
                'is_mutual_followership': mutuality
            }])

            df = pd.concat([df, new_row], ignore_index=True)
            logging.info(f"Processed tweet ID: {tweet_data['id']} for user: {user}")

    logging.info("Scraping process completed")
    return df

async def main(credentials_file, users_path, text_limit, path_to_save):
    logging.info("Main function started")
    df = await scrap_tweets(credentials_file, users_path, int(text_limit))
    df['created_at'] = pd.to_datetime(df['created_at'])
    df = df.sort_values(by=['user', 'created_at'])
    df.to_csv(path_to_save, index=False)
    logging.info(f"Data saved to {path_to_save}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('credentials', metavar='FILE', type=str, help='Path to the credentials text file')
    parser.add_argument('users_path', metavar='FILE', type=str, help='Path to the users list file')
    parser.add_argument('text_limit', metavar='LIMIT', type=int, help='Text limit')
    parser.add_argument('path_to_save', metavar='FILE', type=str, help='Path to save the generated scraping file')

    args = parser.parse_args()

    asyncio.run(main(args.credentials, args.users_path, args.text_limit, args.path_to_save))
