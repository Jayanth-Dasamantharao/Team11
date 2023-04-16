from pymongo import MongoClient
from sqlalchemy import create_engine, text

#Print user details
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
    del user_details['id']
    
    print(user_details)  
    
	
#Top n popular users based on followers count
def top_popular_users():
    n = input("Enter number of users: ")
    results = con.execute(text(f"SELECT id, name, screen_name, followers_count, friends_count FROM users order by followers_count desc limit {n}"))
    user_details = []

    for row in results:
        temp_dic = {}
        temp_dic['name'] = row[1]
        temp_dic['screen_name'] = row[2]
        temp_dic['followers_count'] = row[3]
        temp_dic['friends_count'] = row[4]
        user_details.append(temp_dic)

    for user in user_details:
        print(user)

# Display tweets for given tweets 
def user_tweets():
    name = input("Enter user screen name:")
    choice = int(input("Enter 1 for most recent tweets, 2 for most popular tweets"))
    n = int(input("Enter how many tweets: "))
    results = con.execute(text(f"SELECT id FROM users where screen_name = '{name}' "))
    for row in results:
        id = row[0]
                
    if choice == 1:            
        col = db.tweets.find({"user_id": id}).sort("created_at", -1)
                 
    elif choice == 2:
        col = db.tweets.find({"user_id": id}).sort("retweet_count", -1)  
                 
    else:
        return
    
    count = 0
    for i in col:
        if count < n:
            print({k:v for k, v in i.items() if k not in ['_id', 'user_id']})
            count += 1
            
            
#Number of tweets by an user
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
             
# prints top active users        
def top_active_users():
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

    elif len(user_ids) == 1:
        stmt = f"SELECT id, name, screen_name, followers_count, friends_count FROM users where id = {user_ids[0]}"

    else:
        stmt = f"SELECT id, name, screen_name, followers_count, friends_count FROM users where id in {tuple(user_ids)}"

    results = con.execute(text(stmt))
    user_details = []

    for i, row in enumerate(results):
        temp_dic = {}
        temp_dic['name'] = row[1]
        temp_dic['screen_name'] = row[2]
        temp_dic['followers_count'] = row[3]
        temp_dic['friends_count'] = row[4]
        temp_dic['number_of_tweets'] = number_of_tweets[i]
        user_details.append(temp_dic)

    for user in user_details:
        print(user)
        
#Search by word and displays most retweeted documents       
def search_by_word():
    word = input("Enter a word: ")
    n = int(input("Enter number of top tweets: "))
    count = 0
    tweets = db.tweets.find({"text" : {"$regex" : word}}).sort("retweet_count",-1)
    for i in tweets:
        if count < n:
            print(i)
            count += 1
            
#Search by hashtags and displays most retweeted documents       
def search_by_hashtag():
    hashtag = input("Enter a hashtag: ")
    n = int(input("Enter number of top tweets: "))
    count = 0
    tweets = db.tweets.find({"hashtags" : hashtag}).sort("retweet_count",-1)
    for i in tweets:
        if count < n:
            print(i)
            count += 1
            
		
if __name__ == 'search_app':
    client = MongoClient("mongodb://localhost:27017")
    db = client["twitter"]
    engine = create_engine('postgresql://postgres:root@localhost:5432/twitter')
    con = engine.connect()