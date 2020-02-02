import pandas as pd
import numpy as np
import warnings
import random
from operator import itemgetter
from dp import *
warnings.filterwarnings('ignore')
def predict_based_on_topk (k, sim, training_set):
    
    # Initialize a prediction matrix:   
    prediction_matrix = training_set.values.copy()
    
    for user in range(sim.shape[0]):
      '''
        # for every missing value of this user
        item_set=np.where(training_set.values[user,:]==0)[0]
      '''
      for item in training_set:
        #print(item)
        u_dis=training_set.values[:,item-1].nonzero()[0]
        u_dis_copy=np.float64(u_dis.copy())
        if len(u_dis)!=1:
          for i in range(len(u_dis)):
            u_dis_copy[i]=sim[user][int(u_dis[i])]
            index_of_top_k = [np.argsort(u_dis_copy)[-2:-k-2:-1]]
            index_u=u_dis[index_of_top_k]
                # Denominator is the sum of similarity for each user with its top k users:
            denominator = np.sum(u_dis_copy[index_of_top_k])              
                # Numerator
            numerator = u_dis_copy[index_of_top_k].dot(training_set.values[:,item-1][index_u.astype('int')])
            try:
                if denominator==0 :
                    prediction_matrix[user, item-1] = 0
                else:
                    prediction_matrix[user, item-1] = numerator/denominator
            except:
                pass
        else:
          prediction_matrix[user, item-1]=training_set.values[:,item-1][user]
    return prediction_matrix

def get_final_rec(acc_id,score_mx,id_list,tag_mx,dis_mx,nsim):
    final={}
    r=[]
    #print(float(acc_id) ,type(id_list))
    if float(acc_id) not in id_list:
        for i in range(len(score_mx)):
            final[i]=score_mx[i][0]
        rec=sorted(final.items(),key=itemgetter(1),reverse=True)
        #print(type(rec[0][0]))
        for i in range(len(rec)):
            if len(r)==3:
                break
            else:
                print(int(id_list[rec[i][0]]),int(acc_id))
                if int(id_list[rec[i][0]])!=int(acc_id):
                    r.append(rec[i])
    else:
        index=id_list.index(float(acc_id))
        print(index)
        print(dis_mx.shape)
        acc_sim=np.reshape(tag_mx[index],(len(tag_mx),1))
        dis_mx=np.reshape(dis_mx[index],(len(dis_mx),1))
        print(dis_mx.shape)
        for i in range(len(acc_sim)):
            final[i]=(acc_sim[i][0]*score_mx[i][0]*dis_mx[i][0]+score_mx[i][0])/(acc_sim[i][0]*dis_mx[i][0]+nsim[index][i])
        rec=sorted(final.items(),key=itemgetter(1),reverse=True)
        for i in range(len(rec)):
            if len(r)==3:
                break
            else:
                print(int(id_list[rec[i][0]]),int(acc_id))
                if int(id_list[rec[i][0]])!=int(acc_id):
                    r.append(rec[i])
    print(r)
    return r
