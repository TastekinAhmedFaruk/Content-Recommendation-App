import pyodbc
import pandas as pd 
import time
import datetime
import uuid
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from user_based_data import df_B, ids, watched_list, ratings_list, topVodsAdultUId, topVodsAdultScore, topVodsChildUId, topVodsChildScore,  getSeriesId, all_vods, active_films, child_content, indices_uid, cosine_sim

path = r'Driver={ODBC Driver 17 for SQL Server};Server=XXX.XXX.XXX.XXX; Database=DBNAME; uid=DBUSER; pwd=USERPASS;'


#DB connection
def getConnection(auto):
    return pyodbc.connect(path,autocommit=auto)

conn = getConnection(True)
cursor = conn.cursor()

query_getUserWatched = """ SQL PROCEDURE THAT RETURNS A LIST OF CONTENT THAT USERS WATCH LESS THAN 1 MINUTE OR DO NOT WATCH """
query_recTotal = """  SQL QUERY TO CHECK IF PROPOSED FOR USER """

#Vod-based function used to suggest vod
#uuid: uuid of watched movie, item_: normalized duration, and Calculated cosine similarity values ​​in ItemBasedData.py
#The difference from itembased is that each received similarity value is multiplied by duration.
def get_recommendations_userBased(uuid, item_, cosine_sim=cosine_sim):
    simil_UId=[]
    w_scores =[]
    """ Similarity calculation was done using the cosine function"""
    return simil_UId.values.tolist(), w_scores
#Numpy can also be used to reduce multidimensional lists to one size, but I didn't want to complicate the already messy code any more.
def rearrangeUid(list_):
    temp = []
    for i in range(len(list_)):
        temp.extend(list_[i])
    return temp
#We do not use functions that single out duplicate elements, it is handled with dataframe build-in functions
def rearrangeScore(list_): 
    temp = []
    for i in range(len(list_)):
        temp.extend(list_[i])
    return temp

#Gets the id of user and returns the appropriate content list to recommend to the user
def GetUserWatchedVod(id_):
    cursor.execute(query_getUserWatched, int(id_))
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    return film_ids

#According to the userid, the user has an index between the ids. Since this index is the same in the others, that is, the watched movies, 
#favorite, and duration lists, their lists are also obtained. The number of watched movies and weighted similars are working for each watched movie, 
#and the resulting list is sorted and listed using a dataframe. being returned
def getUserRecommendation(id_):
    userRecsUid = []
    Scores = []
    index = ids.index(id_)
    movies_ = watched_list[int(index)]
    duration_ = ratings_list[int(index)]
    for i in range(len(movies_)):
        if movies_[i] in all_vods:
            uid_, scores_ = get_recommendations_userBased(movies_[i], duration_[i])
            userRecsUid.append(uid_)
            Scores.append(scores_)
        elif(getSeriesId(movies_[i])>0):
            seriesId = getSeriesId(movies_[i])
            temp_uid = str(seriesId).replace('(','').replace(", )","").replace("'","")
            uid_, scores_ = get_recommendations_userBased(temp_uid, duration_[i])
            userRecsUid.append(uid_)
            Scores.append(scores_)
        else:            
            return topVodsAdultUId,topVodsAdultScore,topVodsChildUId,topVodsChildScore
    if (len(movies_)==0):        
        return topVodsAdultUId,topVodsAdultScore,topVodsChildUId,topVodsChildScore
    getUserWatched=GetUserWatchedVod(id_)
    df_result = pd.DataFrame(columns=['Uid','Scores'])
    df_result['Uid'] = rearrangeUid(userRecsUid)
    df_result['Scores'] = rearrangeScore(Scores)
    result_sum = df_result.groupby(['Uid'])['Scores'].sum().reset_index()    
    result_sum = result_sum[result_sum['Uid'].isin(getUserWatched)]
    final_sorted = result_sum.sort_values(by='Scores', ascending = False)
    uid_list = final_sorted['Uid'].values.tolist()
    score_list = final_sorted['Scores'].values.tolist()
    for_child = final_sorted[final_sorted['Uid'].isin(child_content)]
    childUid_list = for_child['Uid'].values.tolist()
    childScore_list = for_child['Scores'].values.tolist()
    final_result_adult = pd.DataFrame(columns=['Uid','Scores'])
    final_result_child = pd.DataFrame(columns=['Uid','Scores'])
    final_result_adult['Uid'] = uid_list[:12]
    final_result_adult['Scores'] = score_list[:12]
    final_result_child['Uid'] = childUid_list[:12]
    final_result_child['Scores'] = childScore_list[:12]
    final_result_adult = final_result_adult.drop_duplicates()
    final_result_child = final_result_child.drop_duplicates()
    return final_result_adult['Uid'].values.tolist(), final_result_adult['Scores'].values.tolist(),final_result_child['Uid'].values.tolist(), final_result_child['Scores'].values.tolist()

#The movie returning from Get User Recommendation is printed to the db according to the uid list, score list and userid.
def writeIntoDB(uuid_list, score_list, id_, isChild):
    try:
        temp_list=[]
        temp_list2=[]
        cursor.execute(query_recTotal, id_)
        results_ = cursor.fetchall()
        temp_list.append(results_)
        temp_list2=rearrangeUid(temp_list) 
        if(len(temp_list2)>1):
            cursor.execute( """ SQL QUERY WRITTEN FOR CLEANING PROPOSITIONS THAT HAVE BEEN MADE """, int(isChild) , int(id_))
        for  i in range(len(uuid_list)):
            date_time = datetime.datetime.now()
            cursor.execute(  """ SQL QUERY WRITTEN FOR ADDING NEW PROPOSALS """,
                          int(id_), int(uuid_list[i]),int(isChild), float(score_list[i]),date_time)
    except Exception as ex:
        print("Error write db : ", ex)
if __name__ == "__main__":
    try:
        for item in ids:
            uuids_adult, scores_adult, uuids_child, scores_child = getUserRecommendation(item)
            if len(uuids_adult)==0 or len(scores_adult)==0 or len(uuids_child)==0 or len(scores_child)==0:
                print("Something Wrong")
            writeIntoDB(uuids_adult, scores_adult, item, '0')
            writeIntoDB(uuids_child, scores_child, item, '1')
    except Exception as ex:
        print("ERROR: ",ex)
    finally:
        cursor.close()
        conn.close()