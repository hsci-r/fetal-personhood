from pathlib import Path
import pandas as pd

DATA_DIR = Path('../disc-analysis/data/input/parquet/')
# filenames that start with abortion
files = [f for f in DATA_DIR.iterdir() if f.is_file() and f.name.startswith('abortion')]

tweets = pd.read_parquet(files[1])
authors = pd.read_parquet(files[7])

tweets_not_retweet = tweets[tweets['retweet_of'].isnull()]
tweets_end_nodes = tweets_not_retweet[tweets_not_retweet['reply_count'] == 0]
tweets_not_op_end_nodes = tweets_end_nodes[tweets_end_nodes["in_reply_to"].notnull()]

query = ['abortion', 'fetus', 'fetal']

discarded_tweet_chains = []
master_tweet_ids = []

for i, row in tweets_not_op_end_nodes.iterrows():
        tweet_ids = []
        current_tweet = tweets_not_op_end_nodes.iloc[i]

        while True:
                print(current_tweet["tweet_id"])
                print(current_tweet["text"])
                tweet_ids.append(current_tweet['tweet_id'])
                previous_tweet_id = current_tweet['in_reply_to']
                previous_tweet = tweets[tweets['tweet_id'] == previous_tweet_id].squeeze()
                if previous_tweet.empty or (previous_tweet['error'] == "Not Found Error"):
                        discarded_tweet_chains.append((current_tweet["tweet_id"], current_tweet['in_reply_to']))
                        tweet_ids = []
                        break

                current_tweet = previous_tweet
                if tweet_ids is not None:
                        master_tweet_ids.append(tweet_ids)

#%%

# map the tweet ids to the actual tweets, into a dataframe
result_list = []
for i, chain in enumerate(master_tweet_ids):
        for j, post in enumerate(chain):
                result_list.append((post, i, j))

df_ids_of_chains = pd.DataFrame(result_list, columns=['tweet_id', 'chain_id', 'post_id'])

df_tweets_of_chains = pd.merge(df_ids_of_chains.astype({'tweet_id': str}), tweets.astype({'tweet_id': str}), on='tweet_id')

# remove duplicate tweet_ids
df_tweets_of_chains = df_tweets_of_chains.drop_duplicates(subset='tweet_id')

#group by chain_id and select chains that contain the query words
df_chains = df_tweets_of_chains.groupby('chain_id').filter(lambda x: x['text'].str.contains('|'.join(query), na=False, case=False).any())

df_chains_no_errors = df_chains.groupby('chain_id').filter(lambda x: not x['error'].str.contains('Authorization').any())

df_preview = df_chains_no_errors[['chain_id', 'post_id', 'tweet_id', 'author_id', 'text', 'in_reply_to']]
df_preview['author_username'] = df_chains_no_errors['author_id'].map(authors.set_index('user_id')['username'])
df_preview.to_csv("tweet_chains.csv", index=False)

