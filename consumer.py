from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import sys
import svd
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

# spark = SparkSession.builder.appName('consumer').getOrCreate()
spark = SparkSession.builder.appName('consumer').config("spark.mongodb.input.uri", "mongodb://127.0.0.1/articles.documents").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/articles.documents").getOrCreate()
brokers, topic = sys.argv[1:]
consumer = KafkaConsumer(topic, bootstrap_servers = ['localhost:9092'])

df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/articles.documents").load()
df.show()



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
for msg in consumer:
    article = msg.value
    for row in df:
        count += 1
        print(count)
        #if (row != row2):
        matching_score = textRankof2docs(article["text"], row["text"])
        print(matching_score)
        if (matching_score < .5):
            data = {}
            data['authors'] = article['authors']
            data['date_publish'] = article['date_publish']
            data['title'] = article['title']
            data['text'] = article['text']
            doc = db.documents.find(
                {"text": article["text"]})
            print(doc)
            if (doc != ''):
                x = col.insert_one(data)
