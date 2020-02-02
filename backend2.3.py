import json
import csv
import datetime
import pandas as pd
import numpy as np
import cf
from pymongo import MongoClient
import datetime

import dp
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import inputs
from flask_restplus import reqparse
from flask_restplus import abort
from flask_cors import CORS
import writeinDB


#get start
writeinDB.prepro()
df_l=['id', 'last_scraped', 
'picture_url','host_id', 'host_name', 'host_response_rate', 
'host_is_superhost', 'street', 
'neighbourhood', 'latitude', 'longitude', 
'is_location_exact', 'property_type', 'accommodates', 
'bathrooms', 'bedrooms', 'beds', 
'amenities','price', 'minimum_nights', 
'maximum_nights', 'review_scores_rating', 'review_scores_accuracy', 
'review_scores_cleanliness', 'review_scores_checkin', 
'review_scores_communication', 'review_scores_location', 
'review_scores_value', 'reviews_per_month', 'booked']   
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
                'last_scraped' ]

extra=['host_response_rate',
           'host_is_superhost',
           'minimum_nights',
           'accommodates',
           'is_location_exact',
           'maximum_nights']
user=['uid','uname','pwd','utype']
upro=['uid','fst_name','lst_name','phone','pic_url']
ulike=['uid','roomid']
reservation=['uid','rid','st_date','fn_date','state','hid','tol_price']

app = Flask(__name__)
CORS(app)
api = Api(app,default = 'AIRBNB',title = 'mini AIRBNB')

model=api.model('Rooms',{'picture_url': fields.String,
            'host_id': fields.Integer,

            'street': fields.String,
            'latitude': fields.Float,
            'longitude': fields.Float,
            'neighbourhood': fields.String,
            'property_type': fields.String,
            'bathrooms': fields.Float,
            'bedrooms': fields.Integer,
            'beds': fields.String,
            'amenities': fields.String,
            'price': fields.String

    })

u_model=api.model('User',{
                    'uname': fields.String,
                    'pwd': fields.String
                    })
login_model=api.model('login',{'uname': fields.String,
                                'pwd': fields.String
                                })
upro_model=api.model('U_pro',{'uid': fields.Integer,
                                'fst_name': fields.String,
                                'lst_name': fields.String,
                                'phone': fields.String,
                                'pic_url': fields.String
                                })
privacy_model=api.model('Privacy',{'uid': fields.Integer,
                                    'opwd': fields.String,
                                    'npwd': fields.String
                                })
review_model=api.model('Review',{'listing_id':fields.Integer,

                                    'reviewer_id':fields.Integer,
                                    'date':fields.String,
                                    #fields.DateTime,
                                    'comments':fields.String,
                                    'review_scores_accuracy':fields.String,
                                    'review_scores_cleanliness':fields.String,
                                    'review_scores_checkin':fields.String,
                                    'review_scores_communication':fields.String,
                                    'review_scores_location':fields.String,
                                    'review_scores_value':fields.String
                                    
                                })
def load(df):
    temp=[]
    items=df.find().limit(2500)
    if df==db['review']:
        for dict in items:
            del dict['_id']
            temp.append(dict)
    else:
        key=0
        for i in items:
            del i['_id']
            temp.append({str(key):i})
            key+=1
    
    a=pd.read_json(json.dumps(temp[0])).T
    for i in temp[1:]:
        b=pd.read_json(json.dumps(i)).T
        a=pd.concat([a, b])
    if df=='Sydney':
        a.set_index('picture_url', inplace=True)
        a.dropna(how='any', inplace=True)
    return a
    


client = MongoClient('mongodb://z5150021:z5150021@ds213832.mlab.com:13832/9900?retryWrites=false')
db = client['9900']
#ulike=[{'uid':11,'rid':000},{'uid':12,'rid':000},{'uid':13,'rid':000},{'uid':0,'rid':12351}]
#writeinDB.built_ulike(ulike)
df= load(db['Sydney'])#
udf=load(db['users'])#users
pro_df=load(db['upro'])#
review_df=load(db['review'])#review
uroom_df=load(db['reservation'])#reservation
ulike_df=load(db['ulike'])#ulike


#preprocessing

##
##for i in range(len(df['price'])):
##    temp=df['price'].iloc[i][1:].split(',')
##    if len(temp)>1:
##        p=float(1000*float(temp[0]))+float(temp[1])
##    else:
##        p=float(temp[0])
##    df['price'].iloc[i]=p
print('length of df: ',len(df))
print(df.columns)
score,tag,id_list,dis=dp.data(df)
print(len(score))
dis_mx,tag_mx,sim=dp.get_mx(dis,score,tag)

nsim=dp.get_item_sim(sim)
f=cf.predict_based_on_topk(5,nsim,score)

score_mx=np.reshape(f.T[0],(len(f),1))

id_list=id_list.tolist()

parser = reqparse.RequestParser()
par_upro = reqparse.RequestParser()
par_single_room = reqparse.RequestParser()
par_ulike = reqparse.RequestParser()
par_update=reqparse.RequestParser()
#print(df['last_scraped'])
parser.add_argument('neighbourhood', choices=list(set(c for c in df['neighbourhood'])))
parser.add_argument('price1')
parser.add_argument('price2')
parser.add_argument('bedrooms')
parser.add_argument('data1')
parser.add_argument('data2')

par_upro.add_argument('uid')
par_single_room.add_argument('id')
par_ulike.add_argument('uid')
par_ulike.add_argument('rid')

par_update.add_argument('price')
par_update.add_argument('rid')
#booking
#-----------------------------------------------------------------------------------------------------------------------------
booking_model=api.model('Booking',{'uid':fields.Integer,
         'rid':fields.Integer,
         'st_date':fields.String,
         'fn_date':fields.String
        })
@api.route('/booking')
class Booking(Resource):
 @api.response(200, 'Success')
 @api.response(411, 'Missing information!')
 @api.response(417, 'Time period not available!') 
 @api.expect(booking_model)
 def post(self):
    b = request.json      #{'uid':fields.Integer,'rid':fields.Integer,'st_date':fields.String, 'fn_data':fields.String }
    b['uid']=int(b['uid'])
    for k in b:
        if b[k]=='':
            abort(411, 'Missing information!')
    tol_day=(datetime.datetime.strptime(b['fn_date'], '%Y%m%d')-datetime.datetime.strptime(b['st_date'], '%Y%m%d')).days
    hid = df[(df['id']==int(b['rid']))]['host_id'].values[0]
    p = df[(df['id']==int(b['rid']))]['price'].values[0]
    tol_price='$'+str(tol_day*int(p))
    period=[i for i in range(int(b['st_date']),int(b['fn_date'])+1)]
    booked_list=json.loads(df[df['id']==int(b['rid'])]['booked'].values[0])['b']
    index=df[df['id']==int(b['rid'])].index
    for i in period:
        if i in booked_list:
            abort(417, 'Time period not available!')
    for i in period:
        booked_list.append(i)
    
    ind=df[df.loc[:,'id'].isin([int(b['rid'])])].index
    r=df.loc[index,:].T.to_dict('dict')
    print(r)
    #print(type(r[i[0]]))
    dic={}
    for j in df_l:
        print(j)
        print(r[ind.values[0]][j])
        dic[j]=r[ind.values[0]][j]
    i={"b":booked_list}
    df.loc[ind,'booked']=json.dumps(i)
    print({str(ind.values[0]):dic})
    dic['booked']=json.dumps(i)
    #find= db['Sydney'].find_one({str(index.values[0]):dic})
    #print(find)       
    #nfind=find
    #nfind[str(index.values[0])]['booked']=json.dumps(i)

    up={"$set":dic}
    print(up)
    #print(nfind)
    index=len(uroom_df)
    for k in b:
        uroom_df.loc[index, k] = int(b[k])
    uroom_df.loc[index,'hid'] = hid
    uroom_df.loc[index,'state'] = 'unpaid'
    uroom_df.loc[index,'tol_price'] = tol_price
    new=pd.DataFrame(uroom_df.tail(1),columns=reservation)
    df2=json.loads(new.to_json())
    for i in df2:
        for j in df2[i]:
            df2[i]=df2[i][j]
    print(df2)
    db['reservation'].insert_one(df2)
    db['Sydney'].update_one(dic,up)
    #print(db['Sydney'].find_one(nfind))
    
    #df[df['id']==b['rid']]['booked']=json.dumps(i)
    return {
                'token':200
        }
    
@api.route('/get_booking')
class Get_booking(Resource):
    global uroom_df
    global pro_df
    global df
    @api.response(200, 'Success')
    @api.response(411, 'Missing information!') 
    @api.expect(par_upro, validate=True)
    def get(self):
        global uroom_df
        global pro_df
        global df
        args = par_upro.parse_args()
        uid = str(args.get('uid'))
        df1=uroom_df[(uroom_df['uid']==int(uid))]
        rl=df1['rid'].values
        print(len(rl))
        re=json.loads('{"token":200}')
        for i in range(len(rl)):
            d=uroom_df[(uroom_df['rid']==int(rl[i]))]
            print(d,len(d)==len(rl),len(d))
            dj=d.to_json(orient='records')
            re2=json.loads(dj)
            print(type(re2),re2)
            print(df[df.loc[:,'id'].isin([int(rl[i])])])
            index=df[df.loc[:,'id'].isin([int(rl[i])])].index
            
            print('index',index)
            print(df.loc[index,'picture_url'])
            pic=df.loc[index,'picture_url'].values[0]
            for j in range(len(re2)):
                re2[j]['picture_url']=pic
            print(re2)
            try:
                re.append(re2)
            except:
                re=re2
        print(re)
        return re
##---------------------------------------------------------------------------------------------------------------------
@api.route('/delete')
class Delete(Resource):
    global df
    @api.response(200, 'Success')
    @api.expect(par_ulike, validate=True)
    def post(self):
        global df
        global udf
        args = par_ulike.parse_args()
        uid = int(str(args.get('uid')))
        rid = int(str(args.get('rid')))
        print(int(rid))
        print()
        acc_d=df[(df['host_id']==uid)&(df['id']==int(rid))]
        print(acc_d)
        i=acc_d.index
        print(i)
        r=df.loc[i,:].T.to_dict('dict')
        df=df.drop(index=i)
        
        
        dic={}
        for j in df_l:
            dic[j]=r[i.values[0]][j]
        db['Sydney'].delete_one({str(i.values[0]):dic})
        
        return {
            'token':200
        }


review_cols=['review_scores_rating',
            'review_scores_accuracy',
            'review_scores_cleanliness',
            'review_scores_checkin',
            'review_scores_communication',
            'review_scores_location',
            'review_scores_value',
            'reviews_per_month']

@api.route('/add_or_refresh')
class Rooms(Resource): 
    global udf
    @api.response(200, 'Success')
    @api.response(411, 'Missing information!')   
    @api.expect(model)
    def post(self):
        global udf
        new = request.json
        pic=new['picture_url']
        print(new['host_id'],type(new['host_id']))
        print(pro_df[pro_df.loc[:,'uid']==new['host_id']]['fst_name'].values)
        uname=pro_df[pro_df.loc[:,'uid']==int(new['host_id'])]['fst_name'].values[0]   
        for k in new:
            if new[k]=='':
                abort(411, 'Missing information!')
        id =len(df)
        df.loc[id,'id']=df['id'].max()+1
        dindex=len(df)

        for key in new:
            if key!='picture_url':
                df.loc[id, key] = new[key]
        df.loc[id, 'host_name']=uname
        df.loc[id, 'host_id']=int(new['host_id'])
        df.loc[id, 'price']=int(new['price'])
        df.loc[id, 'bedrooms']=int(new['bedrooms'])
        #df.loc[id, 'street']=new['street'].split(',')[-1]
        for k in review_cols:
            df.loc[id, k] = 0
        #df.append(new, ignore_index=True)
        a=json.dumps({'b':[]})
        print(a)
        df.loc[id,'booked']=a
        #  df.to_csv('acc_test.csv',encoding = "utf-8")
        uindex=udf[udf.loc[:,'uid'].isin([int(new['host_id'])])].index
        udf.loc[uindex, 'utype']='1'
        
        myquery = { 'uid': int(new['host_id']) }
        find= db['users'].find_one(myquery)
        print(find)
        nfind=find
        nfind['utype']='1'
        up={"$set":nfind}
        db['users'].update_one(find,up)
        new=pd.DataFrame(df.tail(1),columns=remain_col+review_cols+extra+['booked'])
        print(new['booked'])
        new['booked']=a
        #print(new)
        n_j=new.T.to_dict('dict')
        print(n_j)
        #print(n_j)
        #print(type(new['picture_url']))
        for i in range(len(extra)):
            #print(type(extra[i]))
            #print(type(n_j[new['picture_url']]))
            if i!=1 and i!=4:
                n_j[id][extra[i]]=2
            else:
                n_j[id][extra[i]]='t'
        n_j[id]['picture_url']=pic
        n_j[id]['last_scraped']='20191020'
        print(n_j)
        
        db['Sydney'].insert_one(n_j[id])
        print("!!!")
        return {
        'room_id':df.loc[id,'id'],
        'user_id':new['host_id'].values[0],
        'utype':'1',
        'token':200
        }

#-----------------------------------------------------------------------------------------------------------------
@api.route('/payment')
class Payment(Resource):
    @api.response(200, 'Success')
    @api.expect(booking_model)
    def post(self):
        b = request.json 
        df1=uroom_df[(uroom_df['uid']==int(b['uid']))&(uroom_df['rid']==int(b['rid']))]
        print(df1)
        df2=df1[(df1['st_date']==int(b['st_date']))&(df1['fn_date']==int(b['fn_date']))]
        print(type(df1['st_date'].values[0]),type(df1['fn_date'].values[0]))
        print(df2)
        index=df2[uroom_df['uid']==int(b['uid'])].index
        o_d=pd.DataFrame(uroom_df.loc[index,:],columns=reservation).to_dict('dict')
        for i in o_d:
            for j in o_d[i]:
                o_d[i]=o_d[i][j]
        uroom_df.loc[index,'state'] = 'paid'
        #myquery = { "uid": b['uid'], "rid": b['rid'], "st_date": "unpaid", "fn_data": b['fn_data'] }
        find= db['reservation'].find_one(o_d)
        nfind=find
        nfind['state']='paid'
        up={"$set":nfind}
        print(up)
        db['reservation'].update_one(o_d,up)
        return {
            'token':200
        }


#-----------------------------------------------------------------------------------------------------------------
#street  ,price ,bedrooms  data('last_scraped' )

@api.route('/search')
class Rooms2(Resource):
    @api.response(200, 'Success')
    @api.response(408, 'Missing Bedroom value')
    @api.response(405, 'Missing price values')
    @api.response(406, 'Missing data1')
    @api.response(410, 'Missing neighbourhood value')
    @api.expect(parser, validate=True)
    def get(self):
        # get books as JSON string
        args = parser.parse_args()

        # retrieve the query parameters

        neighbourhood = str(args.get('neighbourhood'))
        price1 = args.get('price1')
        price2 = args.get('price2')
        bedrooms= args.get('bedrooms')
        data1 = str(args.get('data1'))
        data2 = str(args.get('data2'))
        if neighbourhood == 'None':
            abort(410,'Missing neighbourhood value')
        elif bedrooms==None:
            abort(408,'Missing Bedroom value')
        elif price1==None or price2==None:
            abort(405,'Missing price values')
        elif data1 == 'None':
            abort(406,'Missing data1')
        else:
            
            df1=df[(df['bedrooms']==int(bedrooms))&(df['neighbourhood']==neighbourhood)]
            df2=df1[(int(price1)<=df['price'])&(df['price']<=int(price2))]
            
            json_str = df2.to_json(orient='records')
            re = json.loads(json_str)
            
        return re

#-----------------------------------------------------------------------------------------------------------------
@api.route('/single_room')
class Single_room(Resource):
    global df
    global review_df
    global score_mx
    global tag_mx
    global id_list
    global nsim
    @api.response(200, 'Success')
    @api.expect(par_single_room, validate=True)
    def get(self):
        global df
        global review_df
        global score_mx
        global tag_mx
        global id_list
        global nsim
        args = par_single_room.parse_args()
        rid = str(args.get('id'))
        index=df[df.loc[:,'id'].isin([int(rid)])].index
        df1=df.loc[index,:]
        print(df1['picture_url'])
        #i=str(index).split(',')[0][8:-2]
        #print(str(index).split(',')[0][8:-2])
        json_str = df1.to_json(orient='records')
        re = json.loads(json_str)
        #print(type(re),re)
        re[0]['token']=200
        #re[0]['picture_url']=i
        #print(re[0])
        print(review_df.columns)
        df2=review_df[(review_df['listing_id']==int(rid))]
        json_str = df2.to_json(orient='records')
        re2 = json.loads(json_str)
        re.append(re2)
        #print(re)
        rec=cf.get_final_rec(rid,score_mx,id_list,tag_mx,dis_mx,nsim)
        print(rec)
        rec_id=[]
        for i in rec:
            a,b=i
            n=df[(df['id']==int(id_list[a]))]
            nj=n.to_json(orient='records')
            #index=df[df.loc[:,'id'].isin([int(id_list[a])])].index
            #i=str(index).split(',')[0][8:-2]
            re3=json.loads(nj)
            print(re3)
            #re3[-1]['picturl_url']=i
            re.append(re3)
        return re
#-----------------------------------------------------------------------------------------------------------------
@api.route('/my_acc')
class My_acc(Resource):
    global df
    @api.response(200, 'Success')
    @api.expect(par_upro, validate=True)
    def get(self):
        global df
        args = par_upro.parse_args()
        uid = str(args.get('uid'))
        df1=df[(df['host_id']==int(uid))]
        print(df1)
        rl=df1['id'].values
        print(rl)
        re=json.loads('{"token":200}')
        for i in rl:
            d=df[(df['id']==int(i))]
            print(d)
            dj=d.to_json(orient='records')
            re2=json.loads(dj)
            #index=df[df.loc[:,'id'].isin([int(i)])].index
            #i=str(index).split(',')[0][8:-2]
            #re2[-1]['picture_url']=i
            try:
                re.append(re2)
            except:
                re=re2

        return re
#-----------------------------------------------------------------------------------------------------------------
@api.route('/like')
class Like(Resource):
    global ulike_df
    @api.response(200, 'Success')
    @api.expect(par_ulike, validate=True)
    def post(self):
        global ulike_df
        args = par_ulike.parse_args()
        uid = str(args.get('uid'))
        rid = str(args.get('rid'))
        df=ulike_df[(ulike_df['uid']==int(uid))&(ulike_df['roomid']==int(rid))]
        print(ulike_df)
        if len(df)==0:
            index=len(ulike_df)+1
            ulike_df.loc[index,'uid']=int(uid)
            ulike_df.loc[index,'roomid']=int(rid)
            #ulike_df=ulike_df.append(pd.DataFrame({'uid':uid,'rid':rid},index=[len(ulike_df)]))
            new=pd.DataFrame(ulike_df.tail(1),columns=ulike)
            print(new)
            print(ulike_df)
            df2=json.loads(new.T.to_json())
            print(df2)
            i={'uid':int(uid),'roomid':int(rid)}
            db['ulike'].insert_one(i)
            print()
        else:
            i=df.index
            ulike_df=ulike_df.drop(index=i)
            db['ulike'].delete_one({'uid':int(uid),'roomid':int(rid)})
        return {
                'token':200
        }
#-----------------------------------------------------------------------------------------------------------------
@api.route('/get_like')
class Get_like(Resource):
    global ulike_df
    @api.response(200, 'Success')
    @api.expect(par_upro, validate=True)
    def get(self):
        global ulike_df
        args = par_upro.parse_args()
        uid = str(args.get('uid'))
        print(ulike_df[(ulike_df['uid']==int(uid))])
        df1=pd.DataFrame(ulike_df[(ulike_df['uid']==int(uid))],columns=ulike)['roomid']
        print(df1)
        rlist=df1.values
        print(rlist)
        re=json.loads('{"token":200}')
        print(re,type(re))
        for i in rlist:
            print('i is: ',i)
            d=df[(df['id']==int(i))]
            dj=d.to_json(orient='records')
            re2=json.loads(dj)
            #index=df[df.loc[:,'id'].isin([int(i)])].index
            #i=str(index).split(',')[0][8:-2]
            #re2[-1]['picture_url']=i
            try:
                re.append(re2)
            except:
                re=re2
        #print(re)     
        return re

#-----------------------------------------------------------------------------------------------------------------
@api.route('/host_order')
class Host_order(Resource):
    @api.response(200, 'Success')
    @api.response(411, 'Missing information!') 
    @api.expect(par_upro, validate=True)
    def get(self):
        global uroom_df
        global pro_df
        global df
        args = par_upro.parse_args()
        uid = str(args.get('uid'))
        df1=uroom_df[(uroom_df['hid']==int(uid))]
        rl=df1['rid'].values
        ul=(df1['uid'].values).tolist()
        print(type(ul))
        for i in range(len(ul)):
            print(ul[i])
        re=json.loads('{"token":200}')
        for i in range(len(rl)):
            d=uroom_df[(uroom_df['rid']==int(rl[i]))]
            print(d)
            dj=d.to_json(orient='records')
            re2=json.loads(dj)
            index=df[df.loc[:,'id'].isin([int(i)])].index
            pic=df.loc[index,'picture_url']
            re2[-1]['picture_url']=pic
            print(type(ul[i]))
            d_1=pro_df[(pro_df['uid']==ul[i])]
            print(d_1)
            fs_name=d_1['fst_name'].values[0]
            lst_name=d_1['lst_name'].values[0]
            phone=d_1['phone'].values[0]
            name=fs_name+' '+lst_name
            re2[-1]['customer']=name
            re2[-1]['phone']=phone
            try:
                re.append(re2)
            except:
               re=re2
        print("!!!",re)
        return re
#-----------------------------------------------------------------------------------------------------------------
@api.route('/post_review')
class Post_review(Resource):
    global review_df
    global pro_df
    global uroom_df
    @api.response(200, 'Success')
    @api.response(414, 'Missing comments')
    @api.expect(review_model)
    def post(self):
        global review_df
        global pro_df
        global uroom_df
        n_c=request.json
        index=pro_df[pro_df['uid']==int(n_c['reviewer_id'])].index
        print(index)
        print(pro_df.loc[index,'fst_name'])
        r_n=pro_df.loc[index,'fst_name'].values.tolist()
        print(r_n[0])
        if len(review_df)==0:
            cid=1
        else:
            cid=review_df['id'].max()+1
        if n_c['comments']=='':
            abort(414, 'Missing comments')
        else:
            review_df=review_df.append(pd.DataFrame({'listing_id':n_c['listing_id'],
                                    'id':cid,
                                    'reviewer_id':n_c['reviewer_id'],
                                    'date':n_c['date'],
                                    'reviewer_name':r_n,
                                    'comments':n_c['comments']}))
            index=df[df.loc[:,'id'].isin([int(n_c['listing_id'])])].index
            r=df.loc[index,:].T.to_dict('dict')
            new_rate=(float(n_c['review_scores_accuracy'])+float(n_c['review_scores_cleanliness'])+float(n_c['review_scores_checkin'])+float(n_c['review_scores_communication'])+float(n_c['review_scores_location'])+float(n_c['review_scores_value']))/6*10
            
            #print(df.loc[index,:].T)
            df.loc[index,'review_scores_rating']=float(df.loc[index,'review_scores_rating'].values[0])*0.99+0.01*new_rate
            
            #print(r[index.values[0]])
            dic={}
            for i in df_l:
                dic[i]=r[index.values[0]][i]
            #print(dic)
            find= db['Sydney'].find_one(dic)
            print(find)
            for i in find:
                print(i)
            nfind=find
            nfind['review_scores_rating']=new_rate
            up={"$set":nfind}
            db['Sydney'].update_one(dic,up)
            
        new=pd.DataFrame(review_df.tail(1),columns=ulike)
        df2=json.loads(new.T.to_json())
        for i in df2.keys():
            db['review'].insert_one({i:df2[i]})
        return {
                'token':200
        }
#-----------------------------------------------------------------------------------------------------------------
@api.route('/signup')
class Signup(Resource):
    global udf
    @api.response(200, 'Success')
    @api.response(400, 'Missing Username/Password')
    @api.response(409, 'Username Taken')
    @api.expect(u_model)
    def post(self):
        global udf
        new_user = request.json
        
        #print(type(new_user['uname']),type(new_user['pwd']))
        if len(udf)==0:
            uid=1
        else:
            uid=udf['uid'].max()+1
        #udf.loc[uid,'uid']=uid
        if new_user['uname'] in udf['uname'].values:
            abort(409, 'Username Taken')
        if new_user['uname']=='' or new_user['pwd']=='':
            abort(400, 'Username and password cannot be empty')
        index=len(udf)
        for i in new_user:
            udf.loc[index,i]=new_user[i]
        udf.loc[index,'utype']='0'
        udf.loc[index,'uid']=uid
        #udf=udf.append(pd.DataFrame({'uid':uid,'uname':new_user['uname'],'pwd':new_user['pwd'],'utype':'0'},index=[len(udf)]))
        #t=pd.DataFrame({'uid':uid,'uname':new_user['uname'],'pwd':new_user['pwd'],'utype':'0'},index=[len(udf)])
        #udf=pd.concat[udf,t]
        new=pd.DataFrame(udf.tail(1),columns=user)
        
        df2=json.loads(new.to_json())
        for i in df2:
            for j in df2[i]:
                df2[i]=df2[i][j]
        
        db['users'].insert_one(df2)
        #print(udf.tail(1))
        #re=json.dumps({'uid':str(uid),'utype':'0'})
        #print(type(re))
        return {
            'uid':str(uid),
            'utype':'0',
            'token':200
        }
#-----------------------------------------------------------------------------------------------------------------
@api.route('/login')
class Login(Resource):
    global udf
    @api.response(200, 'Success')
    @api.response(400, 'Missing Username/Password')
    @api.response(403, 'Invalid Username')
    @api.response(401, 'Invalid Password')
    @api.expect(login_model)
    def post(self):
        global udf
        log = request.json
        #print(log)
        if log['uname']=='' or log['pwd']=='':
            abort(400, 'Missing Username/Password')
        uinfo=udf.loc[udf['uname']==log['uname']]
        if log['uname'] not in udf['uname'].values:
            abort(403, 'Invalid Username')
        elif str(log['pwd'])!=str(uinfo[uinfo['uname'].isin([log['uname']])]['pwd'].values.tolist()[0]):
            abort(401, 'Invalid Password')
        else:
            uid=str(uinfo[uinfo['uname'].isin([log['uname']])]['uid'].values[0])
            utype=str(uinfo[uinfo['uname'].isin([log['uname']])]['utype'].values[0])
            re=json.dumps({'uid':uid,'utype':utype})
            print(re)
            return {
            'uid':uid,
            'utype':utype,
            'token':200
        }
#-----------------------------------------------------------------------------------------------------------------
@api.route('/profile')
class Profile(Resource):
    global pro_df
    @api.response(200, 'Success')
    @api.response(411, 'Missing information!') 
    @api.expect(upro_model)
    def post(self):
        global pro_df
        pro = request.json
        pro['uid']=int(pro['uid'])
        print(pro)
        for k in pro:
            if k!='pic_url':
                if pro[k]=='':
                    abort(411, 'Missing information!')

        if pro['uid'] in pro_df['uid'].values:
            index=pro_df[pro_df.loc[:,'uid'].isin([pro['uid']])].index
            o_d=pd.DataFrame(pro_df.loc[index,:],columns=upro).to_dict('dict')
            for i in o_d:
                for j in o_d[i]:
                    o_d[i]=o_d[i][j]
            #print(o_d)
            for k in pro:
                if k!='uid':
                    pro_df.loc[index, k] = pro[k]
            myquery = { "uid": pro['uid'] }
            find= db['upro'].find_one(o_d)
            print(find)
            nfind=find
            nfind['fst_name']=pro['fst_name']
            nfind['lst_name']=pro['lst_name']
            nfind['phone']=pro['phone']
            nfind['pic_url']=pro['pic_url']
            up={"$set":nfind}
            print(nfind)
            db['upro'].update_one(o_d,up)
            return {
                'token':203
            }
        else:
            index=len(pro_df)
            for i in pro:
                pro_df.loc[index,i]=pro[i]
            print(pro_df)
            new=pd.DataFrame(pro_df.tail(1),columns=upro)
            print(json.loads(new.to_json()))
            df2=json.loads(new.to_json())
            for i in df2:
                for j in df2[i]:
                    df2[i]=df2[i][j]

            print(df2)
            
            db['upro'].insert_one(df2)
            return {
                'token':203
            } 
        print(pro_df)
        
        return {
            'token':300
        }
#-----------------------------------------------------------------------------------------------------------------
@api.route('/get_profile')
class Profile_get(Resource):
    global pro_df
    @api.response(200, 'Success')
    @api.response(201, 'Success')
    @api.expect(par_upro, validate=True)
    def get(self):
        args = par_upro.parse_args()
        uid = str(args.get('uid'))
        #print(uid,type(uid))
        #rint(type(pro_df['uid'].values[0]),pro_df['uid'].values[0])
        print('sss',len(pro_df[pro_df.loc[:,'uid'].isin([int(uid)])]))
        if len(pro_df[pro_df.loc[:,'uid'].isin([int(uid)])])!=0:

            index=pro_df[pro_df.loc[:,'uid'].isin([int(uid)])].index
        else:
            return {
                'token': 201
            }
        #print(index)
        df1=pro_df.loc[index,:]
        try:
            df1=df1.drop(columns=['Unnamed: 0'])
        except:
            pass
        json_str = df1.to_json(orient='records')
        re = json.loads(json_str)
        print(type(re),re)
        re[0]['token']=200
        print(re[0])
        return re[0]
#-----------------------------------------------------------------------------------------------------------------
@api.route('/privacy')
class Privacy(Resource):
    global udf
    @api.response(200, 'Success')
    @api.response(401, 'Invalid Password')
    @api.response(411, 'Missing information!') 
    @api.response(413, 'Two passwords cannot be the same') 
    @api.expect(privacy_model)
    def post(self):
        global udf
        pri = request.json
        for k in pri:
            if pri[k]=='':
                abort(411, 'Missing information!')
        index=udf[udf.loc[:,'uid'].isin([pri['uid']])].index
        o_d=pd.DataFrame(pro_df.loc[index,:],columns=upro).to_dict('dict')
        for i in o_d:
            for j in o_d[i]:
                o_d[i]=o_d[i][j]

        #print(type(udf.loc[index, 'pwd'].values[0]),type(pri['opwd']))
        if str(udf.loc[index, 'pwd'].values[0])!=pri['opwd']:
            abort(401, 'Invalid Password')
        if pri['opwd']==pri['npwd']:
            abort(413, 'Two passwords cannot be the same')
        #index=udf[udf.loc[:,'uid'].isin([pri['uid']])].index
        udf.loc[index, 'pwd'] = pri['npwd']
        myquery = { "uid": pri['uid'] }
        #print(o_d)
        #print(db['users'].find_one())
        find= db['users'].find_one(myquery)
        nfind=find
        nfind['pwd']=pri['npwd']
        print(nfind)
        up={"$set":nfind}
        db['users'].update_one(myquery,up)
        return 200
#-----------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':

    #df.set_index('picture_url', inplace=True)
    # run the application
    app.run(debug=True)


