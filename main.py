#create spark job
# 1.
# https://stackoverflow.com/questions/38271611/how-to-convert-json-string-to-dataframe-on-spark


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import sys
import svd
import json
# from svd import *
import string
from pyspark.ml.feature import HashingTF, IDF,  Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import udf
from scipy.stats import linregress
from scipy import sparse
import numpy as np
import spacy
from pymongo import MongoClient
# Connect to the MongoDB, change the connection string per your MongoDB environment
client = MongoClient("mongodb://localhost:27017/")
# Set the db object to point to the business database
# db=client.articles
db = client["articles"]
col = db["documents"]

nlp = spacy.load('es_core_news_sm')
from textRankKeyword import *

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('pipeline').getOrCreate()

line = spark.read.json("result1.json", multiLine=True)

# Extracts the main content of the article and related metadata (i.e headline, author, date, published)

drop_list = ['date_download', 'date_modify', 'description', 'filename', 'image_url', 'language', 'localpath', 'source_domain','title_page','title_rss', 'url']
line = line.select([column for column in line.columns if column not in drop_list]).alias('text')

line.show(5)
# tokenizer = Tokenizer(inputCol="text", outputCol="words")
# remover = StopWordsRemover(inputCol="words", outputCol="filtered")
# hashingTF = HashingTF(inputCol="filtered", outputCol="rawfeatures", numFeatures=10000)
# idf = IDF(inputCol="rawfeatures", outputCol="features")
# pipeline = Pipeline(stages=[tokenizer,remover,hashingTF,idf])
#
# (trainData, testData) = line.randomSplit([0.7, 0.3])
# model = pipeline.fit(trainData)
# predictions = model.transform(testData)
#
# predictions.show(5)

# def calculatePearson(s1, s2):
#     # print(type(s1['features']))
#     s1 = s1["features"].toArray()
#     s2 = s2["features"].toArray()
#     jac = np.dot(s1,s2)/(np.linalg.norm(s1)*np.linalg.norm(s2))
#     print(jac)
#     return 1
#
# count = 0
# # calculatePearson_udf = udf(calculatePearson)
# for row1 in predictions.rdd.toLocalIterator():
#     for row2 in predictions.rdd.toLocalIterator():
#         count += 1
#         print(count)
#         calculatePearson(row1, row2)
# # predictions.rdd.map(lambda row: calculatePearson(row))

def textRankof2docs(d1, d2):
    d1_res = TextRank4Keyword()
    d1_res.analyze(d1, candidate_pos = ['NOUN', 'PROPN','VERB'], window_size=4, lower=False)
    d1_words = d1_res.get_keywords(30)
    print("*********")
    d2_res = TextRank4Keyword()
    d2_res.analyze(d2, candidate_pos=['NOUN', 'PROPN', 'VERB'], window_size=4, lower=False)
    d2_words = d2_res.get_keywords(30)
    if d1_words == None or d2_words == None:
        c_len = 0
    else:
        c = list(set(d1_words) & set(d2_words))
        c_len = len(c) #save mongodb here
        if(c_len/30 >= 0.15):
            embedding_path = "/Users/aditya/PycharmProjects/BigDataProject/SBW-vectors-300-min5.txt"
            d1_list = []
            d1_list.append(d1)
            d2_list = []
            d2_list.append(d2)
            m_score = svd.mapping(embedding_path,d1_list,d2_list)
            return m_score[0][0]
    return 0

count = 0
for row1 in line.rdd.toLocalIterator():
    for row2 in line.rdd.toLocalIterator():
        count += 1
        print(count)
        if(row1!=row2):
            matching_score = textRankof2docs(row1["text"], row2["text"])
            print(matching_score)
            if (matching_score < .5):
                data = {}
                data['authors'] = row1['authors']
                data['date_publish'] = row1['date_publish']
                data['title'] = row1['title']
                data['text'] = row1['text']
                doc = db.documents.find({"text" : row1["text"]}) #Ye line chud rahi hai. Check nhi kar paa rha ki db me hai ya nahi.
                print(doc)
                if(doc != ''):
                   x = col.insert_one(data)





