import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

# Print user details
def user_details(): 
    name = input("Enter user screen name: ")
    results = con.execute(text(f"SELECT id, name, screen_name, followers_count, friends_count FROM users where screen_name = '{name}' "))
    user_details = {}
    
    for row in results:
        user_details['id'] = row[0]
        user_details['name'] = row[1]
        user_details['screen_name'] = row[2]
        user_details['followers_count'] = row[3]
        user_details['friends_count'] = row[4]
   
    agg_results = no_of_tweets()
    for i in agg_results:
        if i['_id'] == user_details['id']:
            user_details['num_of_tweets'] = i['num_of_tweets']
    
    columns = ['name', 'screen_name', 'followers_count', 'friends_count', 'num_of_tweets']
    to_dataframe([user_details], columns, 1)

    
# Top n popular users based on followers count
def top_popular_users():
    n = int(input("Enter number of users: "))
    results = con.execute(text(f"SELECT id, name, screen_name, followers_count, friends_count FROM users order by followers_count desc limit {n}"))
    user_details = []

    for row in results:
        temp_dic = {}
        temp_dic['name'] = row[1]
        temp_dic['screen_name'] = row[2]
        temp_dic['followers_count'] = row[3]
        temp_dic['friends_count'] = row[4]
        user_details.append(temp_dic)

    columns = ['name', 'screen_name', 'followers_count', 'friends_count']
    to_dataframe(user_details, columns, n)

        
# Display tweets for given tweets 
def user_tweets():
    name = input("Enter user screen name:")    
    n = int(input("Enter how many tweets: "))
    results = con.execute(text(f"SELECT id FROM users where screen_name = '{name}' "))
    for row in results:
        id = row[0]
        
    tweets = db.tweets.find({"user_id": id})             
    tweets = order_results(tweets)

    columns = ['created_at', 'text', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'hashtags', 'user_mentions']
    to_dataframe(tweets, columns, n)

            
# Prints top active users        
def most_tweeted_users():
    n = int(input("Enter number of active tweets:"))
    agg_result = no_of_tweets()
    count = 0
    number_of_tweets = []
    user_ids = []
    
    for i in agg_result:
        if count < n:
            number_of_tweets.append(i['num_of_tweets'])
            user_ids.append(i['_id'])
            count += 1 
            
    if len(user_ids) <= 0:
        print("Please enter valid number")
        return

    else:
        user_ids = ",".join(str(user) for user in user_ids)
        
        stmt = f"""SELECT u.id, u.name, u.screen_name, u.followers_count, u.friends_count FROM users u
                   JOIN unnest(array[{user_ids}]) with ordinality as l(id, idx) ON u.id = l.id
                   ORDER BY l.idx"""

    results = con.execute(text(stmt))
    user_details = []

    for i, row in enumerate(results):
        temp_dic = {}
        temp_dic['name'] = row[1]
        temp_dic['screen_name'] = row[2]
        temp_dic['followers_count'] = row[3]
        temp_dic['friends_count'] = row[4]
        temp_dic['num_of_tweets'] = number_of_tweets[i]
        user_details.append(temp_dic)
        
    columns = ['name', 'screen_name', 'followers_count', 'friends_count', 'num_of_tweets']
    to_dataframe(user_details, columns, n)

        
# Search by word and displays most retweeted documents       
def search_by_word():
    word = input("Enter a word: ")
    n = int(input("Enter number of top tweets: "))
    count = 0
    tweets = db.tweets.find({"text" : {"$regex" : word}})
    tweets = order_results(tweets)
    columns = ['_id', 'created_at', 'text', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'hashtags', 'user_mentions']
    to_dataframe(tweets, columns, n)

            
# Search by hashtags and displays most retweeted documents       
def search_by_hashtag():
    hashtag = input("Enter a hashtag: ")
    n = int(input("Enter number of top tweets: "))
    count = 0
    tweets = db.tweets.find({"hashtags" : hashtag})
    tweets = order_results(tweets)
    columns = ['created_at', 'text', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'hashtags', 'user_mentions']

    to_dataframe(tweets, columns, n)

# Displays Top N popular tweets    
def popular_hashtags():
    tweets = db.tweets.aggregate([
                    { "$unwind": "$hashtags" },
                    { "$group": {
                        "_id": "$hashtags",
                        "count": { "$sum": 1 }
                    }}, 
                    {"$sort" : {"count": -1} },
                    { "$replaceRoot": { "newRoot": { "hashtag": "$_id" , "num_of_tweets": "$count"} } }
                ])
    
    n = int(input("Enter number of top hashtags to display: "))
    columns = ['hashtag', 'num_of_tweets']
    to_dataframe(tweets, columns, n)

            
# Number of tweets by an user
def no_of_tweets():

    agg_result= db.tweets.aggregate(
    [{
    "$group" : 
        {"_id" : "$user_id", 
         "num_of_tweets" : {"$sum" : 1}   
        }},
        {
    "$sort" : {"num_of_tweets": -1}
    }]
     )
    
    return agg_result


# Ordering the results based on the input parameter
def order_results(tweets):
    choice = int(input("Enter 1 for most recent tweets, 2 for most popular tweets. 3 for tweets in date range: "))
    if choice == 1:
        return tweets.sort("created_at", -1)
    
    elif choice == 2:
        return tweets.sort("retweet_count", -1)
    
    elif choice == 3:
        max_date_inc = input("Enter maximum date(yyyy-mm-dd): ")
        min_date_exc = input("Enter minimum date(yyyy-mm-dd): ")
        tweets = db.tweets.find({
            "created_at": {
                "$gt": min_date_exc,
                "$lt":  max_date_inc
            }
        }).sort("created_at", -1)
        return tweets

    else:
        return []

    
# Converting dictionary to dataframe
def to_dataframe(tweets, columns, n):
    df = pd.DataFrame(columns = columns)
    count = 0
    for i in tweets:
        if count < n:
            temp_dic = {k:v for k, v in i.items() if k in columns}
            temp_df = pd.DataFrame(temp_dic.values(), index = temp_dic.keys()).T
            df = pd.concat([df, temp_df])
            count += 1
    df = df.reset_index().drop("index", axis = 1)
    
    with pd.option_context('display.max_colwidth', None):
        display(df.head(n))
        
        
if __name__ == 'search_app':
    client = MongoClient("mongodb://localhost:27017")
    db = client["twitter"]
    engine = create_engine('postgresql://postgres:root@localhost:5432/twitter')
    con = engine.connect()