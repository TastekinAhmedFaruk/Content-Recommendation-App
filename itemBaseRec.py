import pyodbc
import pandas as pd 
import time
import datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from itembasedata_for_one_day import df_B, active_films, child_content, indices_uid, cosine_sim

path = r'Driver={ODBC Driver 17 for SQL Server};Server=XXX.XXX.XXX.XXX; Database=DBNAME; uid=DBUSER; pwd=USERPASS;'

#DB connection
def getConnection(auto):
    return pyodbc.connect(path,autocommit = auto)

conn = getConnection(True)
cursor = conn.cursor()

#Vod-based function used to suggest vod
#Gets the cosine similarity values ​​calculated and returned from the ItemBasedData.py script and the UUID of the vod for which the suggestion is to be obtained
#Indices_uid In the pandas dataframe, the line of the movie whose suggestion is to be taken is passed to the idx so that the movie and score values ​​will not be mixed after sorting.
#Movies sorted according to their score, their uidleri and scores are returned as a list
#The iloc function passes the uids of movies similar to simil_uid with indexes obtained from scores. iloc is used here to get rows by index
def get_recommendations_itemBased(uuid, cosine_sim=cosine_sim):
    simil_UId=[]
    w_scores =[]
    """ Similarity calculation was done using the cosine function"""
    return simil_UId.values.tolist(), w_scores

#Incoming values are processed back through the dataframe because it is easier to filter and manipulate
#With the tilde (~) isin method, the movie is prevented from suggesting itself. Since it is a note statement, the relevant values are dropped from the dataframe.
#With the tildaless isin method, the movies in active_films were suggested, and the others were dropped, the same process was done in chil_content (child profile)
#In similar cases, only the first 10 movies will be bought and printed.
#Duplicate data is extracted from the list created for the child profile and adult profile, and the remaining list is returned sequentially.
def getItemRecommendation(Uid_):
    movies_ = [Uid_]
    uid_, scores_ = get_recommendations_itemBased(Uid_)
    df_result = pd.DataFrame(columns=['Uid','Scores'])
    df_result['Uid'] = uid_
    df_result['Scores'] = scores_
    df_result = df_result[df_result['Uid'].isin(active_films)]
    df_result = df_result[~df_result['Uid'].isin(movies_)]
    uid_list = df_result['Uid'].values.tolist()
    score_list = df_result['Scores'].values.tolist()
    child_df = df_result[df_result['Uid'].isin(child_content)]
    childUid_list = child_df['Uid'].values.tolist()
    childScore_list = child_df['Scores'].values.tolist()
    final_result_adult = pd.DataFrame(columns=['Uid','Scores'])
    final_result_child = pd.DataFrame(columns=['Uid','Scores'])
    final_result_adult['Uid'] = uid_list[:10]
    final_result_adult['Scores'] = score_list[:10]
    final_result_child['Uid'] = childUid_list[:10]
    final_result_child['Scores'] = childScore_list[:10]
    final_result_adult = final_result_adult.drop_duplicates()
    final_result_child = final_result_child.drop_duplicates()
    return final_result_adult['Uid'].values.tolist(), final_result_adult['Scores'].values.tolist(),final_result_child['Uid'].values.tolist(), final_result_child['Scores'].values.tolist()

#Prints the list and score list returned from getItemReccoendation to the DB
def writeIntoDB(uuid_list, score_list, ischild, movieId):     
    try:        
        cursor.execute(""" SQL QUERY WRITTEN FOR CLEANING PROPOSITIONS THAT HAVE BEEN MADE """, movieId, ischild)
        for  i in range(len(uuid_list)):
            date_time = datetime.datetime.now()
            if float(score_list[i]) > 0:                
                cursor.execute(""" SQL QUERY WRITTEN FOR ADDING NEW PROPOSALS """
                ,movieId, uuid_list[i], ischild, float(score_list[i]),date_time)
    except Exception as ex:
        print("ERROR: ", ex)

startTime = time.time()

if __name__ == "__main__":
    try:
        #print("A similar matrix was created. Creating Similars for each movie and printing to DB : ", datetime.datetime.now())
        for item in active_films:
            uuids1, scores1, uuids2, scores2 = getItemRecommendation(item)
            if len(uuids1) == 0 or len(scores1) == 0 or len(uuids2) == 0 or len(scores2) == 0:
                print("Something Wrong")
            writeIntoDB(uuids1, scores1, '0', item)
            writeIntoDB(uuids2, scores2, '1', item)
        #print("Dataset is written to DB. The total time: {} seconds ".format(time.time() - startTime))
    except Exception as ex:
        print("ERROR:", ex)
    finally:
        cursor.close()
        conn.close()