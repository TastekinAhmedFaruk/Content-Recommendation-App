import pyodbc
import pandas as pd 
import time
import datetime
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

path = r'Driver={ODBC Driver 17 for SQL Server};Server=XXX.XXX.XXX.XXX; Database=DBNAME; uid=DBUSER; pwd=USERPASS;'

query_allFilms = """ SQL QUERY RETURNING THE ID LIST OF ALL CONTENT (SERIES & MOVIE) """
query_activeFilms = """ SQL QUERY RETURNING THE ID LIST OF ACTIVE CONTENT (SERIES & MOVIE) """
query_childContent = """ SQL QUERY RETURNING THE ID LIST OF ACTIVE CHILD CONTENT (SERIES & MOVIE) """
query_vodTitle = """ SQL QUERY RETURNING THE TITLE LIST OF ALL CONTENT (SERIES & MOVIE) """
query_vodKeywords = """ SQL QUERY RETURNING THE KEYWORD LIST OF ALL CONTENT (SERIES & MOVIE) """
query_subVodCategories = """ SQL QUERY RETURNING THE SUBCATEGORY LIST OF ALL CONTENT (SERIES & MOVIE) """
query_vodCastings = """ SQL QUERY RETURNING THE CREDITS LIST OF ALL CONTENT (SERIES & MOVIE) """
query_provider = """ SQL QUERY RETURNING THE PROVIDER LIST OF ALL CONTENT (SERIES & MOVIE) """

#DB connection 
def getConnection(auto):
    return pyodbc.connect(path,autocommit=auto)

conn = getConnection(True)
cursor = conn.cursor()

#Gets Id of all Videos and returns as list
def getAllVods(query_=query_allFilms):
    cursor.execute (query_)
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    #print("Tüm vodlar alındı : ",datetime.datetime.now())
    return film_ids

#Gets the ids of all active movies and returns as a list
def getActiveFilms(query_=query_activeFilms):
    cursor.execute(query_)
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    #print("Tüm aktif vodlar alındı : ",datetime.datetime.now())
    return film_ids

#Gets the Id of all active child movies and returns as a list
def getChildContent(query_=query_childContent):
    cursor.execute(query_)
    results = cursor.fetchall()
    film_ids = []
    for value in results:
        vodId = str(value[0]).replace('(','').replace(", )","").replace("'","")
        film_ids.append(vodId)
    #print("List has been prepared of child movies: ",datetime.datetime.now())
    return film_ids

#Gets list of movie ids and returns the provider of movies as a list
def getProviders(list_):
    film_providers = []
    for film in list_:
        cursor.execute(query_provider, film)
        row = str(cursor.fetchone()).replace('(','').replace(", )","").replace("'","").strip('"').strip().replace("\t", "").replace("\\t", "").lower()
        film_providers.append(row)
    #print("List has been prepared of providers : ",datetime.datetime.now())
    return film_providers

#Gets list of movie ids and returns the titles of movies as a list
def getTitles(list_):
    film_titles = []
    for film in list_:
        cursor.execute(query_vodTitle, film)
        row = str(cursor.fetchone()).replace('(','').replace(", )","").replace("'","").strip('"').strip().replace("\t", "").replace("\\t", "").lower()
        film_titles.append(row)
    #print("List has been prepared of Vod Title : ",datetime.datetime.now())
    return film_titles

#Gets list of movie ids and returns the keywords of movies as a list
def getKeywords(list_):
    film_keywords = []
    temp_list = []
    for i in range(len(list_)):
        cursor.execute(query_vodKeywords, list_[i])
        keywords_ = cursor.fetchall()
        temp_list.append(keywords_)
    for film in temp_list:
        temp_l = []
        for item in film:
            temp = str(item).replace("(","").replace(", )","").replace("'", "").replace("\\t", "").replace("\\n", "")
            temp_l.append(temp)
        film_keywords.append(temp_l)
    #print("List has been prepared of Vod keyword : ",datetime.datetime.now())
    return film_keywords

#Gets list of movie ids and returns the subcategories of movies as a list
def getSubCat(list_):
    sub_categories = []
    temp_cat = []
    for i in range(len(list_)):
        cursor.execute(query_subVodCategories, list_[i])
        categories_ = cursor.fetchall()
        temp_cat.append(categories_)
    for film in temp_cat:        
        temp_l = []
        for item in film:
            temp = str(item).replace("(","").replace(", )","").replace("'", "").replace("\\n", "").replace("\\t", "").lower()
            temp_l.append(temp)
        sub_categories.append(temp_l)
    #print("List has been prepared of Vod Subcategories  : ",datetime.datetime.now())
    return sub_categories

#Gets list of movie ids and returns the credits of movies as a list
def getCredits(list_):
    vod_credits = []
    temp_cred = []
    for i in range(len(list_)):
        cursor.execute(query_vodCastings, list_[i])
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
#print("Dataset creating : ",datetime.datetime.now())
df_B, active_films, all_vods, child_content = dataset()

#In keyword, credits, and categories, when any character in the list is in the form of a space character, countvectorization can get rid of spaces and 
#appear as more than one keyword, reducing the odds ratio. Therefore, this function is used to remove spaces and concatenate strings containing spaces.
#Ex: "isin kilici" "isinkilici" is converted into
def clean_data(x):
    if isinstance(x, list):
        return [str.lower(i.strip().replace(" ", "")) for i in x]
features = ['Keywords', 'Credits', 'SubCategory','Providers']

for feature in features:
    df_B[feature] = df_B[feature].apply(clean_data)

#In order to bring together all the data we will use for the similarity calculation, another column named soup is created in the dataframe and all data is thrown into it.
def createSoup(x):
    return ' '.join(x['Keywords']) + ' ' + ' '.join(x['Credits']) + ' ' +  ' '.join(x['SubCategory'])
indices_uid = pd.Series(df_B.index, index=df_B['UId'])

df_B['Soup'] = df_B.apply(createSoup, axis=1)

#Calculate the countvectorization of each vod vector before calculating the cosine similarity. In short, strings turn into mathematical expressions
count = CountVectorizer()
count_matrix = count.fit_transform(df_B['Soup'])
#Then the vod matrix consisting of vectors is multiplied with itself to calculate the similarity of each vod vector with all vods.
cosine_sim = cosine_similarity(count_matrix, count_matrix)

cursor.close()
conn.close()