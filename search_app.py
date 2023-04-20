import json
import threading
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from multiprocessing import Process
from sqlalchemy import create_engine, text


# Print user details
def user_details():
    name = input("Enter user screen name: ")
    
    if f"user_details_{name}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"user_details_{name}"])
        cache_dict[f"user_details_{name}"]['count'] += 1
    else:
        print("from db")
        stmt = f"SELECT id as _id, name, screen_name, followers_count, friends_count FROM users where screen_name = '{name}' "
        df1 = pd.read_sql(text(stmt), con)        
        agg_results = no_of_tweets()
        df2 =  pd.DataFrame(list(agg_results))
        final_df = pd.merge(df1, df2, on = '_id').drop("_id", axis = 1)
        update_cache(cache_dict, f"user_details_{name}", final_df)

    display_df(final_df)

    
# Top n popular users based on followers count
def top_popular_users():
    n = int(input("Enter number of users: "))
    if f"top_popular_users_{n}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"top_popular_users_{n}"])
        cache_dict[f"top_popular_users_{n}"]['count'] += 1
    else:
        print("from db")
        stmt = f"SELECT id as _id, name, screen_name, followers_count, friends_count FROM users order by followers_count desc limit {n}"
        df1 = pd.read_sql(text(stmt), con)
        agg_results = no_of_tweets()
        df2 =  pd.DataFrame(list(agg_results))
        final_df = pd.merge(df1, df2, on = '_id').drop("_id", axis = 1)
        update_cache(cache_dict, f"top_popular_users_{n}", final_df)
    display_df(final_df)

        
# Display tweets for given tweets 
def user_tweets():
    name = input("Enter user screen name:")    
    n = int(input("Enter how many tweets: "))
    order = int(input("Enter 1 to order by date, 2 to order by most retweeted: "))
    range = input("Do you want date range(y|n):")
    max_date, min_date = "", ""
    if range == "y":
        max_date = input("Enter maximum date(yyyy-mm-dd): ")
        min_date = input("Enter minimum date(yyyy-mm-dd): ")
    
    if f"user_tweets_{name}_{n}_{order}_{max_date}_{min_date}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"user_tweets_{name}_{n}_{order}_{max_date}_{min_date}"])
        cache_dict[f"user_tweets_{name}_{n}_{order}_{max_date}_{min_date}"]['count'] += 1
        
    else:    
        print("from db")
        results = con.execute(text(f"SELECT id FROM users where screen_name = '{name}' "))

        for row in results:
            id = row[0]

        if range == "y":
            tweets = db.tweets.find({"user_id": id, 
                "created_at": {
                    "$gt": min_date,
                    "$lt":  max_date}
            })

        else: 
            tweets = db.tweets.find({"user_id": id}) 

        if order == 1:
            tweets.sort("created_at", -1)
        else:
            tweets.sort("retweet_count", -1)

        final_df = pd.DataFrame(list(tweets))
        columns = ['created_at', 'text', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'hashtags', 'user_mentions']
        final_df = final_df.filter(columns)
        update_cache(cache_dict, f"user_tweets_{name}_{n}_{order}_{max_date}_{min_date}", final_df)
    display_df(final_df.head(n))

            
# Prints top active users        
def most_tweeted_users():
    n = int(input("Enter number of active tweets:"))
    if f"most_tweeted_users_{n}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"most_tweeted_users_{n}"])
        cache_dict[f"most_tweeted_users_{n}"]['count'] += 1
        
    else:
        print("from db")
        agg_results = no_of_tweets()
        df1 = pd.DataFrame(list(agg_results))
        df1 = df1.iloc[0: n]
        stmt = f"""select id as user_id, screen_name, followers_count, friends_count from users 
                   where id in  {tuple(df1['_id'].values.tolist())}"""
    
        df2 = pd.read_sql(text(stmt), con) 
        final_df = pd.merge(df2, df1, right_on = '_id', left_on = "user_id").drop(["_id", "user_id"], axis = 1)
        update_cache(cache_dict, f"most_tweeted_users_{n}", final_df)
    display_df(final_df.sort_values(by = "num_of_tweets", ascending = False))

        
# Search by word and displays most retweeted documents       
def search_by_word():
    word = input("Enter a word: ")
    n = int(input("Enter number of top tweets: "))
    order = int(input("Enter 1 to order by date, 2 to order by most retweeted: "))
    range = input("Do you want date range(y|n):")
    max_date, min_date = "", ""
    if range == "y":
        max_date = input("Enter maximum date(yyyy-mm-dd): ")
        min_date = input("Enter minimum date(yyyy-mm-dd): ")
        
    if f"search_by_word_{word}_{n}_{order}_{max_date}_{min_date}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"search_by_word_{word}_{n}_{order}_{max_date}_{min_date}"])
        cache_dict[f"search_by_word_{word}_{n}_{order}_{max_date}_{min_date}"]['count'] += 1
        
    else:
        print("from db")
        if range == "y":
            tweets = db.tweets.find({"text" : {"$regex" : word}, 
                                     "created_at": {
                                        "$gt": min_date,
                                        "$lt":  max_date}
                                            })
        else:
            tweets = db.tweets.find({"text" : {"$regex" : word}})

        if order == 1:
            tweets.sort("created_at", -1)
        else:
            tweets.sort("retweet_count", - 1)

        df = pd.DataFrame(list(tweets))
        df = df.iloc[0: n]
        if len(df) == 0:
            return

        columns = ['screen_name', 'created_at', 'text', 'quote_count', 'reply_count', 'retweet_count', 
                   'favorite_count', 'hashtags', 'user_mentions']
        if n == 1:
            stmt = f"select id as user_id, screen_name from users where id = {df['user_id'].values.tolist()[0]}"

        else:
            stmt = f"select id as user_id, screen_name from users where id in {tuple(df['user_id'].values.tolist())}"

        temp_df = pd.read_sql(text(stmt), con)
        final_df = pd.merge(df, temp_df, on = "user_id")
        final_df = final_df.filter(columns)
        update_cache(cache_dict, f"search_by_word_{word}_{n}_{order}_{max_date}_{min_date}", final_df)
    display_df(final_df)

            
# Search by hashtags and displays most retweeted documents       
def search_by_hashtag():
    hashtag = input("Enter a hashtag: ")
    n = int(input("Enter number of top tweets: "))
    order = int(input("Enter 1 to order by date, 2 to order by most retweeted: "))
    range = input("Do you want date range(y|n):")
    max_date, min_date = "", ""
    if range == "y":
        max_date = input("Enter maximum date(yyyy-mm-dd): ")
        min_date = input("Enter minimum date(yyyy-mm-dd): ")
        
    if f"search_by_hashtag_{hashtag}_{n}_{order}_{max_date}_{min_date}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"search_by_hashtag_{hashtag}_{n}_{order}_{max_date}_{min_date}"])
        cache_dict[f"search_by_hashtag_{hashtag}_{n}_{order}_{max_date}_{min_date}"]['count'] += 1
    
    else:
        print("from db")
        if range == "y":
             tweets = db.tweets.find({"hashtags" : hashtag, 
                                     "created_at": {
                                        "$gt": min_date,
                                        "$lt":  max_date}
                                            })           
        else:
            tweets = db.tweets.find({"hashtags" : hashtag})
   
        if order == 1:
            tweets.sort("created_at", -1)
        else:
            tweets.sort("retweet_count", - 1)
            
        df = pd.DataFrame(list(tweets))
        df = df.iloc[0: n]
        
        if len(df) == 0:
            return
        
        columns = ['screen_name', 'created_at', 'text', 'quote_count', 'reply_count', 'retweet_count', 
                   'favorite_count', 'hashtags', 'user_mentions']

        if n == 1:
            stmt = f"select id as user_id, screen_name from users where id = {df['user_id'].values.tolist()[0]}"

        else:
            stmt = f"select id as user_id, screen_name from users where id in {tuple(df['user_id'].values.tolist())}"

        temp_df = pd.read_sql(text(stmt), con)
        final_df = pd.merge(df, temp_df, on = "user_id")
        final_df = final_df.filter(columns)
        update_cache(cache_dict, f"search_by_hashtag_{hashtag}_{n}_{order}_{max_date}_{min_date}", final_df)
    display_df(final_df)

    
# Displays Top N popular tweets    
def popular_hashtags():
    n = int(input("Enter number of top hashtags to display: "))
 
    if f"popular_hashtags_{n}" in cache_dict:
        print("from cache")
        final_df = pd.DataFrame(cache_dict[f"popular_hashtags_{n}"])
        cache_dict[f"popular_hashtags_{n}"]['count'] += 1
        
    else:
        print("from db")
        agg_results = db.tweets.aggregate([
                        { "$unwind": "$hashtags" },
                        { "$group": {
                            "_id": "$hashtags",
                            "count": { "$sum": 1 }
                        }}, 
                        {"$sort" : {"count": -1} },
                        { "$replaceRoot": { "newRoot": { "hashtag": "$_id" , "num_of_tweets": "$count"} } }
                    ])

        df = pd.DataFrame(list(agg_results))
        final_df = df.iloc[0: n]
        update_cache(cache_dict, f"popular_hashtags_{n}", final_df)
    display_df(final_df)

            
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


def display_df(df):
    with pd.option_context('display.max_colwidth', None):
        display(df)       

        
def update_cache(cache_dict, temp_key, df):
    if len(cache_dict) == 5:
        least_count = cache_dict[list(cache_dict.keys())[0]]['count']
        key_dict = list(cache_dict.keys())[0]
        
        for key, value in cache_dict.items():
            if least_count > cache_dict[key]['count']:
                least_count = cache_dict[key]['count']   
                key_dict = key
                
        cache_dict.pop(key_dict)
     
    cache_dict[temp_key] = df.to_dict()
    cache_dict[temp_key]['count'] = 1
    cache_dict[temp_key]['entry_time'] = str(datetime.now())[:-7]

    
def write_cache():
    with open('cache.txt', 'w') as convert_file:
        convert_file.write(json.dumps(cache_dict))
    threading.Timer(10, write_cache).start()

    
def check_staleness():
    current_time = str(datetime.now())[:-7]
    temp_keys = []
    format = "%Y-%m-%d %H:%M:%S"
    for key in cache_dict.keys():
        print("key", key)
        time1 = datetime.strptime(current_time, format)
        time2 = datetime.strptime(cache_dict[key]['entry_time'], format)
        if (time1 - time2).total_seconds() /60 > 1:
            temp_keys.append(key)
        
    for i in temp_keys:
        cache_dict.pop(i)
   
    threading.Timer(10, check_staleness).start()
             
        
if __name__ == 'search_app':
    client = MongoClient("mongodb://localhost:27017")
    db = client["twitter"]
    engine = create_engine('postgresql://postgres:root@localhost:5432/twitter')
    con = engine.connect()

    with open("cache.txt", "r") as f1:
        cache_dict = {}
        for line in f1:
            try:
                cache_dict = json.loads(line)
            except ValueError:
                pass   
            
    p1 = Process(target = write_cache)
    p1.start()
    p2 = Process(target = check_staleness)
    p2.start()
    write_cache()
    check_staleness()