import json
import pandas as pd
from pymongo import MongoClient
import datetime
import pymongo
import pandas as pd

import warnings

warnings.filterwarnings('ignore')
client = MongoClient('mongodb://z5150021:z5150021@ds213832.mlab.com:13832/9900?retryWrites=false')
db = client['9900']

#main table Sydney
def built_sydney():
    c = db['Sydney']
    remain_col=['id',
                'picture_url',
                'host_id' ,
                'host_name',
                'street',
                'neighbourhood' ,
                'latitude',
                'longitude',
                'amenities',
                'property_type',
                'bathrooms',
                'bedrooms',
                'beds',
                'price',
                'review_scores_rating',
                'review_scores_accuracy',
                'review_scores_cleanliness',
                'review_scores_checkin',
                'review_scores_communication',
                'review_scores_location',
                'review_scores_value',
                'reviews_per_month',
                'last_scraped' ]

    extra=['host_response_rate',
           'host_is_superhost',
           'minimum_nights', 
           'accommodates',
           'is_location_exact',
           'maximum_nights']

    df= pd.read_csv('listings_dec18.csv',usecols=remain_col+extra,nrows=10000)
    #df= pd.read_csv('listings_dec18.csv',usecols=remain_col+extra,nrows=10)
    i={'b':[]}
    a=json.dumps(i)
    df['booked']=a
    ##preprocessing
    df.dropna(how='any', inplace=True)
     #format the price
    for i in range(len(df['price'])):
        temp=df['price'].iloc[i][1:].split(',')
        if len(temp)>1:
            p=float(1000*float(temp[0]))+float(temp[1])
        else:
            p=float(temp[0])
        df['price'].iloc[i]=p
     #format the date
    for i in range(len(df['last_scraped'])):
        df['last_scraped'].iloc[i]=int('20191021')

    ##load into db
    df2=json.loads(df.T.to_json())
    n=0
    for i in df2.keys():
        #c.insert_one({i:df2[i]})
        c.insert_one(df2[i])
        n+=1
        if n==2000:
        #if n==2:
            break

    return db
#table user
def build_users(user):
    c = db['users']
    for i in user:
        c.insert_one(i)
    return

#table user_profile
def build_upro(upro):
    c = db['upro']
    for i in upro:
        c.insert_one(i)
    return


#table ulike
def built_ulike(ulike):
    c = db['ulike']
    for i in ulike:
        c.insert_one(i)
    return


#table reservation
def reserve(reservation):
    c = db['reservation']
    for i in reservation:
        c.insert_one(i)
    return

#table review
def review():
    ##load into db
    review=pd.read_csv('reviews_dec18.csv',nrows=10000)
    c = db['review']
    df2=json.loads(review.T.to_json())
    for i in df2.keys():
        c.insert_one({i:df2[i]})
    return


user=[{'uid':0,'uname':'test1','pwd':'1234','utype':'0'},
          {'uid':1,'uname':'test2','pwd':'1234','utype':'0'},
          {'uid':2,'uname':'test3','pwd':'1234','utype':'0'},
          {'uid':3,'uname':'test4','pwd':'1234','utype':'0'}]
upro=[{'uid':0,'fst_name':'a','lst_name':'Last','phone':'1234567890','pic_url':'sss'},
          {'uid':1,'fst_name':'b','lst_name':'Last','phone':'1234567890','pic_url':'sss'},
          {'uid':2,'fst_name':'c','lst_name':'Last','phone':'1234567890','pic_url':'sss'},
          {'uid':3,'fst_name':'d','lst_name':'Last','phone':'1234567890','pic_url':'sss'}]
          
ulike=[{'uid':0,'roomid':12351},{'uid':0,'roomid':73639},{'uid':0,'roomid':701914},{'uid':0,'roomid':768091}]
reservation=[
                    {'uid':7,'rid':12351,'st_date':20191120,'fn_date':20191125,'state':'unpaid','hid':17061,'tol_price':'$500'},
                    {'uid':0,'rid':14250,'st_date':20191120,'fn_date':20191125,'state':'paid','hid':55948,'tol_price':'$600'},
                    {'uid':0,'rid':15253,'st_date':20190101,'fn_date':20190103,'state':'finished','hid':59850,'tol_price':'$700'}]

#review=pd.read_csv('listings_dec18.csv')
tables=db.list_collection_names(session=None)
def prepro():
    if 'Sydney' not in tables:
        print('insert SYD')
        built_sydney()
        print('SYD DONE')
    if 'users' not in tables:
        build_users(user)
    if 'upro' not in tables:
        build_upro(upro)
    if 'ulike' not in tables:
        built_ulike(ulike)
    if 'reservation' not in tables:
        reserve(reservation)
    if 'review' not in tables:
        review()    
    return

def refresh():
    db['users'].drop()
    build_users(user)
    db['upro'].drop()
    build_upro(upro)
    db['ulike'].drop()
    built_ulike(ulike)
    db['reservation'].drop()
    reserve(reservation)
