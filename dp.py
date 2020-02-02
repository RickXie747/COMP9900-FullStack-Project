import pandas as pd
import numpy as np
import math
import warnings
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

warnings.filterwarnings('ignore')
# dataframe
def data(data):
    print(data)
    col_num=[]
    col=['id','picture_url','host_response_rate','latitude',
         'longitude','host_is_superhost',
         'is_location_exact','property_type','accommodates',
         'bathrooms','bedrooms','beds','price','weekly_price',
         'minimum_nights','maximum_nights','review_scores_rating',
         'review_scores_accuracy', 'review_scores_cleanliness',
         'review_scores_checkin', 'review_scores_communication',
         'review_scores_location', 'review_scores_value','reviews_per_month']
    col_t=['id','host_is_superhost','is_location_exact','property_type','accommodates',
         'bathrooms','bedrooms','beds','minimum_nights','maximum_nights','reviews_per_month']
    col_l=['id','host_is_superhost','is_location_exact','property_type','accommodates',
         'bathrooms','bedrooms','beds','minimum_nights','maximum_nights','reviews_per_month','latitude','longitude']

    df= pd.DataFrame(data,columns=col)
    df_tag=pd.DataFrame(data,columns=col_t)
    #df=df[i:i+2000]
    df_1=df[col_l]
    #print(df_1)
    df_2= df.ix[:,[0,-8,-7,-6,-5,-4,-3,-2,-1]]
    df_2=df_2.fillna(0)
    a=np.array(df_2)
    a=a.tolist()
    newl=[]
    for i in a:
        try:
            if i[-1]>=3:
                newl.append(i)
        except:
            pass
        
    print(len(newl))
    b=np.array(newl)
    c=np.array(df_1)
    c=c.tolist()
    n=[]
    for i in c:
        #print(i[-1])
        try:
            if i[-3]>=3:
                n.append(i)
        except:
            pass
    d=np.array(n)
    df_r=pd.DataFrame(d,columns=col_l).drop(columns=['id','reviews_per_month','latitude','longitude'])

    df_l=pd.DataFrame(d,columns=col_l).drop(columns=col_t)

    le = LabelEncoder()
    le.fit(df_r['host_is_superhost'].drop_duplicates())
    df_r['host_is_superhost']=le.transform(df_r['host_is_superhost'])
    le.fit(df_r['is_location_exact'].drop_duplicates())
    df_r['is_location_exact']=le.transform(df_r['is_location_exact'])
    le.fit(df_r['property_type'].drop_duplicates())
    df_r['property_type']=le.transform(df_r['property_type'])
    
    df_n=pd.DataFrame(b)
    
    id_list=np.array(df_n[0])
    
    le.fit(df_n[0].drop_duplicates())
    df_n[0]=le.transform(df_n[0])
    df_3=df_n.ix[:,[1,2,3,4,5,6,7]]
    return df_n.drop(columns=[0,8]),df_r,id_list,df_l

def get_item_sim(data):
    d=pd.DataFrame(data)
    d.fillna(0,inplace=True)
    for i in range(len(d)):
        d.iloc[i,i] = 1
    return d.values

def get_mx(dis,score,tag):
    dis_l=dis.values
    dis_mx=np.zeros((len(dis),len(dis)))
    sim=score.T.corr(method = 'pearson', min_periods=1).values
    score=score.values
    tag_value=tag.values
    tag_mx=np.zeros((len(tag),len(tag)))
    for i in range(len(tag)-1):
        for j in range(i+1,len(tag),1):
            count=0
            dis_mx[i][j] = 1-math.sqrt((abs(float(dis_l[i][0]))-abs(float(dis_l[j][0])))**2+(abs(float(dis_l[i][1]))-abs(float(dis_l[j][1])))**2)
            dis_mx[j][i] = 1-math.sqrt((abs(float(dis_l[i][0]))-abs(float(dis_l[j][0])))**2+(abs(float(dis_l[i][1]))-abs(float(dis_l[j][1])))**2)
            for k in range(len(tag.columns)):
                if tag_value[i][k]==tag_value[j][k]:
                    count+=1
            tag_mx[i][j]=count/len(tag.columns)
            tag_mx[j][i]=count/len(tag.columns)
        tag_mx[i][i]=1
        dis_mx[i][i]=1
    tag_mx[-1][-1]=1
    dis_mx[-1][-1]=1
    return dis_mx,tag_mx,sim
