import pyodbc
import pandas as pd 
import time
import datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

path = r'Driver={ODBC Driver 17 for SQL Server};Server=XXX.XXX.XXX.XXX; Database=DBNAME; uid=DBUSER; pwd=USERPASS;'

query_activeFilms = """ SQL QUERY RETURNING THE ID LIST OF ACTIVE CONTENT (SERIES & MOVIE) """
query_vodId = """ SQL QUERY RETURNING THE IDS LIST OF CONTENT (MOVIE) """
query_vodTitle = """ SQL QUERY RETURNING THE TITLE LIST OF ALL CONTENT (SERIES & MOVIE) """
query_vodKeywords = """ SQL QUERY RETURNING THE KEYWORD LIST OF ALL CONTENT (SERIES & MOVIE) """
query_subVodCategories = """ SQL QUERY RETURNING THE SUBCATEGORY LIST OF ALL CONTENT (SERIES & MOVIE) """
query_vodCastings = """ SQL QUERY RETURNING THE CREDITS LIST OF ALL CONTENT (SERIES & MOVIE) """
query_provider = """ SQL QUERY RETURNING THE PROVIDER LIST OF ALL CONTENT (SERIES & MOVIE) """
query_childContent = """ SQL QUERY RETURNING THE ID LIST OF ACTIVE CHILD CONTENT (SERIES & MOVIE) """
query_userID = """ SQL PROCEDURE TURNING LIST OF USERS WATCHING WITHIN THE LAST 3 HOURS """
query_durationandWatched = """ A SQL QUERY WITH THE TIMES OF THE CONTENT THAT THE USER MADE """
query_durationandWatchedNew = """ THE SQL PROCEDURE THAT RETURNS THE USER'S CONTENT ALONG WITH THE DURATOIN """
query_topWatchedAdult = """ SQL QUERY RETURNING THE LIST OF 12 HIGHEST ITEMS ACCORDING TO THE RECOMMENDED Score (SERIES & MOVIE) RECOMMENDED TO USERS """
query_topWatchedChild = """ SQL QUERY RETURNING THE LIST OF 12 HIGHEST ITEMS ACCORDING TO THE RECOMMENDED SCORE OF USERS (SERIES & MOVIE) FOR THE CHILD PROFILE """
query_seriesId = """ SQL QUERY RETURNING THE MAIN SECTION INFORMATION OF THE CONTENTS (SERIES) """

#DB connection
def getConnection(auto):
    return pyodbc.connect(path,autocommit=auto)

conn = getConnection(True)
cursor = conn.cursor()

#Gets the ids of all active movies and returns as a list
def getActiveFilms(query_=query_activeFilms):
    cursor.execute(query_)
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    return film_ids

#Gets Id of all Videos and returns as list
def getAllVods(query_=query_vodId):
    cursor.execute (query_)
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    return film_ids

#Gets the Id of all active child movies and returns as a list
def getChildContent(query_=query_childContent):
    cursor.execute(query_)
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    return film_ids

#Gets list of movie ids and returns the provider of movies as a list
def getProviders(list_):
    film_providers = []
    for film in list_:
        cursor.execute(query_provider, film)
        row = str(cursor.fetchone()).replace('(','').replace(", )","").replace("'","").strip('"').strip().replace("\t", "").replace("\\t", "").lower()
        film_providers.append(row)
    return film_providers

#Gets list of movie ids and returns the titles of movies as a list
def getTitles(list_):
    film_titles = []
    for film in list_:
        cursor.execute(query_vodTitle, film)
        row = str(cursor.fetchone()).replace('(','').replace(", )","").replace("'","").strip('"').strip().replace("\t", "").replace("\\t", "").lower()
        film_titles.append(row)
    return film_titles

#Gets list of movie ids and returns the keywords of movies as a list
def getKeywords(list_):
    film_keywords = []
    temp_list = []
    for i in range(len(list_)):
        cursor.execute(query_keywords, list_[i])
        keywords_ = cursor.fetchall()
        temp_list.append(keywords_)
    for film in temp_list:
        temp_l = []
        for item in film:
            temp = str(item).replace("(","").replace(", )","").replace("'", "").replace("\\t", "").replace("\\n", "")
            temp_l.append(temp)
        film_keywords.append(temp_l)
    return film_keywords

#Gets list of movie ids and returns the subcategories of movies as a list
def getSubCat(list_):
    sub_categories = []
    temp_cat = []
    for i in range(len(list_)):
        cursor.execute(query_subCategory, list_[i])
        categories_ = cursor.fetchall()
        temp_cat.append(categories_)
    for film in temp_cat:
        temp_l = []
        for item in film:
            temp = str(item).replace("(","").replace(", )","").replace("'", "").replace("\\n", "").replace("\\t", "").lower()
            temp_l.append(temp)
        sub_categories.append(temp_l)
    return sub_categories

#Gets list of movie ids and returns the credits of movies as a list
def getCredits(list_):
    vod_credits = []
    temp_cred = []
    for i in range(len(list_)):
        cursor.execute(query_credits, list_[i])
        credits_ = cursor.fetchall()
        temp_cred.append(credits_)
    for film in temp_cred:
        temp_l = []
        for item in film:
            temp = str(item).replace("(","").replace(", )","").replace("'", "").replace("\\n", "").replace("\\xa0", "").replace("\\t", "").strip().lower()
            if(temp.find(",") != -1):
                temp1 = temp.split(",")
                for i in temp1:
                    temp_l.append(i.strip())
            else:
                temp_l.append(temp)
        vod_credits.append(temp_l)
    return vod_credits

#Creating dataframes from incoming lists
def buildDF(list_):
    columnNames=['UId', 'Title', 'Keywords', 'SubCategory', 'Credits', 'Providers']
    df_B = pd.DataFrame(columns=columnNames)
    for i in range(len(columnNames)):
        df_B[columnNames[i]]=list_[i]
    return df_B

#Assign lists to a variable and assign it to Dataframe. Then function that creates a dataset that returns the dataframe and other lists we will use
def dataset():
    all_vods = getAllVods()
    active_films = getActiveFilms()
    child_content = getChildContent()
    vod_titles = getTitles(all_vods)
    vod_keywords = getKeywords(all_vods)
    vod_subCat = getSubCat(all_vods)
    vod_credits = getCredits(all_vods)
    vod_providers = getProviders(all_vods)
    column_list = [all_vods, vod_titles, vod_keywords, vod_subCat, vod_credits, vod_providers]
    df_B = buildDF(column_list)
    return df_B, active_films, all_vods, child_content
df_B, active_films, all_vods, child_content = dataset()

#In keyword, credits, and categories, when any character in the list is in the form of a space character, countvectorization can get rid of spaces and 
#appear as more than one keyword, reducing the odds ratio. Therefore, this function is used to remove spaces and concatenate strings containing spaces.
#Ex: "isin kilici" "isinkilici" is converted into
def clean_data(x):
    if isinstance(x, list):
        return [str.lower(i.strip().replace(" ", "")) for i in x]
features = ['Keywords', 'Credits', 'SubCategory']
for feature in features:
    df_B[feature] = df_B[feature].apply(clean_data)
#In order to bring together all the data we will use for the similarity calculation, another column named soup is created in the dataframe and all data is thrown into it.
def createSoup(x):
    return ' '.join(x['Keywords']) + ' ' + ' '.join(x['Credits']) + ' ' + ' '.join(x['SubCategory'])
indices_uid = pd.Series(df_B.index, index=df_B['UId'])
df_B['Soup'] = df_B.apply(createSoup, axis=1)
#Calculate the countvectorization of each vod vector before calculating the cosine similarity. In short, strings turn into mathematical expressions
count = CountVectorizer()
count_matrix = count.fit_transform(df_B['Soup'])
#Then the vod matrix consisting of vectors is multiplied with itself to calculate the similarity of each vod vector with all vods.
cosine_sim = cosine_similarity(count_matrix, count_matrix)

#Reads and processes users from DB and returns as a list
def getUsers(query_=query_userID):
    user_Ids = []
    cursor.execute(query_)
    ids = cursor.fetchall()
    for id_ in ids:
        temp = str(id_).replace('(','').replace(", )","").replace("'","")
        user_Ids.append(temp)
    return user_Ids
#Reads and processes movies watched from DB with their duration and returns them as two separate lists
def getDurandFilm(list_):
    viewed_movies = []
    durations = []
    temp_list = []
    for i in range(len(list_)):
        cursor.execute(query_durationandWatchedNew, list_[i])
        results_ = cursor.fetchall()
        temp_list.append(results_)
    for value in temp_list:
        temp_movie = []
        temp_duration = []
        for item in value:
            temp1 = str(item[0]).replace("(","").replace(", )","").replace("'", "")
            temp2 = str(item[1]).replace("(","").replace(", )","").replace("'", "")
            temp_movie.append(temp1)
            temp_duration.append(temp2)
        viewed_movies.append(temp_movie)
        durations.append(temp_duration)
    return viewed_movies, durations
#Function that normalizes between 0.1-0.5
def rescalling(list_):
    new_durations = []
    """ Normalization processes were carried out to meet customer demands."""
    return new_durations
#If the user has never watched, it returns by sorting from the most watched movies.
def getTopVodsAdult(query_=query_topWatchedAdult):
    cursor.execute(query_)
    results = cursor.fetchall()
    film_ids = []
    film_scores = []
    top_ids = []
    top_scores = []
    x=0
    for value in results:   
        temp1 = str(value[0]).replace("(","").replace(", )","").replace("'", "")
        temp2 = str(value[1]).replace("(","").replace(", )","").replace("'", "")
        top_ids.append(temp1)
        top_scores.append(temp2)
    for item in top_ids:
        if item in active_films:
            film_ids.append(item)
            film_scores.append(top_scores[x])
        x=x+1
        if(len(film_ids)==12):
            break        
    return film_ids,film_scores
#If the user has never watched, it returns by sorting from the most watched child movies.
def getTopVodsChild(query_=query_topWatchedChild):
    cursor.execute(query_)
    results = cursor.fetchall()
    film_ids = []
    film_scores = []
    top_ids = []
    top_scores = []
    x=0
    for value in results:   
        temp1 = str(value[0]).replace("(","").replace(", )","").replace("'", "")
        temp2 = str(value[1]).replace("(","").replace(", )","").replace("'", "")
        top_ids.append(temp1)
        top_scores.append(temp2)
    for item in top_ids:
        if item in active_films:
            film_ids.append(item)
            film_scores.append(top_scores[x])
        x=x+1
        if(len(film_ids)==12):
            break        
    return film_ids,film_scores
#If the vod being watched is an episode, the seriesid is returned as an int, then this seriesid is used to find the uid of the main of the relevant series
def getSeriesId(uid_):
    cursor.execute(query_seriesId, uid_)
    id_ = cursor.fetchone()
    result = str(id_).replace('(','').replace(", )","").replace("'","")
    return int(result)

#print("durations_list,watched_list,ratings_list,topVods,active_films,all_vods,child_content data calculated : ",datetime.datetime.now())
ids = getUsers()
watched_list, durations_list = getDurandFilm(ids)
ratings_list = rescalling(durations_list)
topVodsAdultUId, topVodsAdultScore = getTopVodsAdult()
topVodsChildUId, topVodsChildScore= getTopVodsChild()
active_films=getActiveFilms()
all_vods=getAllVods()
child_content=getChildContent()