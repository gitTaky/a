#from __future__ import print_function

from flask import Flask, request, make_response, url_for
import json
import os
from os.path import join, dirname
from watson_developer_cloud import LanguageTranslationV2 as LanguageTranslation, ToneAnalyzerV3, DocumentConversionV1, TextToSpeechV1
import urllib, urllib2
from cassandra.cluster import Cluster
import sys
from operator import add
from pyspark.sql import SparkSession

'''class TransPassword(urllib2.HTTPPasswordMgr):
    def find_user_password(self, realm, authurl):
        return ('be0889e0-2694-40d2-978a-144accb08dc3', '1LXnqxetPsfY')
'''

app = Flask(__name__)
#app.config['UPLOAD_FOLDER'] = 'tmp'

@app.route("/")
def login():
    tone_analyzer = ToneAnalyzerV3(
        username='6e4c0424-2ac3-4dee-a231-ebff63fab4dc',
        password='NMjsEehEZSe0',
        version='2016-05-19 ')
    text=json.dumps(request.get_json()['text'])
    res=tone_analyzer.tone(text)

    #a= (json.dumps(tone_analyzer.tone(text), indent=2))

    language_translation = LanguageTranslation(
        username='58bcd279-bb41-4e51-adeb-78e60435aad5',
        password='SFOzV3qZaDoM')
    tone=[]
    score=[]
    for i in range(3):
        tone.append(language_translation.translate(
            text=res["document_tone"]["tone_categories"][0]["tones"][i]["tone_name"],
            source='en',
            target='fr'))
        score.append(res["document_tone"]["tone_categories"][0]["tones"][i]["score"])
    #b=b.encode("utf-8")
    #c=c.encode("utf-8")

    cluster = Cluster(['172.17.0.2'])
    session = cluster.connect()
    session.execute("""create keyspace if not exists tone with replication={'class':'SimpleStrategy','replication_factor':'1'};""")
    session.execute("""use tone""")
    session.execute("create table if not exists users (tone_name text ,score float, primary key(tone_name))")
    prepared_stmt = session.prepare ( "INSERT INTO users (tone_name,score) VALUES (?, ?)")
    for i in range(3):
        bound_stmt = prepared_stmt.bind([tone[i], score[i]])
        stmt = session.execute(bound_stmt)
    #session.execute("""insert into demo2.users(id,input)values(request.args.get("id"),request.args.get("text"));""")
    ##session.execute("create table demo.users(tone_name text primary key,score float,tone_id text)")
    #session.execute("""insert into demo.users(tone_name,score,tone_id)values('Anger',a["document_tone"]["tone_categories"]["tones"]["score"],'anger');""")
    #session.execute("""insert into demo.users(tone_name,score,tone_id)values('Disgust',a["document_tone"]["tone_categories"]["tones"]["score"],'disgust');""")
    #session.execute("""insert into demo.users(tone_name,score,tone_id)values('Happy',0.2082342,'happy');""")
    #session.execute("""select * from users;""")
    translation = "tone\tscore\t\n"+tone[0]+"\t"+str(score[0])+"\n"+tone[1]+"\t"+str(score[1])+"\n"+tone[2]+"\t"+str(score[2])
    return translation
    #return json.dumps(translation,indent=2,ensure_ascii=True)

@app.route("/doc",methods=['POST'])
def doc():
    document_conversion = DocumentConversionV1(
        username='f830e53c-6d45-46c8-9349-701d43496d59',
        password='zxojMBxHUVYj',
        version='2016-02-09')

    # Example of retrieving html or plain text

    '''with open(join(dirname(__file__), '../resources/example.html'), 'r') as document:
        config = {'conversion_target': DocumentConversionV1.NORMALIZED_text}
        return (document_conversion.convert_document(document=document, config=config, media_type='text/html')
              .content)
'''
    document = request.files['file']
    config = {'conversion_target': DocumentConversionV1.NORMALIZED_TEXT}
    text = (document_conversion.convert_document(document=document, config=config, media_type='text/html')
            .content)

    text_to_speech = TextToSpeechV1(
        username='6d144da0-eff6-4dc6-bc3b-839a97b7c55a',
        password='GMnlUkKYo8mI',
        x_watson_learning_opt_out=True)  # Optional flag

    #print(json.dumps(text_to_speech.voices(), indent=2))

    wav = text_to_speech.synthesize(text, accept='audio/wav', voice="en-US_AllisonVoice")
    response = make_response(wav, 200)
    response.headers["Content-Disposition"] = "attachment; filename=voice.wav"

    return response

@app.route("/word", methods=['GET', 'POST'])
def word():
    if request.method == 'GET':
        text = request.get_json()['text']
        print text
        filename = 'temp'
        with open(join(join(dirname(__file__), filename)),'w') as temp:
            temp.write(text)
    else:
        text = request.files['file']
        filename = 'temp'
        text.save(join(join(dirname(__file__), filename)))

    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()

    lines = spark.read.text(join(dirname(__file__), filename)).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    output = counts.collect()

    dic = {}
    for (word,count) in output:
        if word != '':
            dic[word] = count

    cluster = Cluster(['172.17.0.2'])
    session = cluster.connect()
    session.execute("""create keyspace if not exists word with replication={'class':'SimpleStrategy','replication_factor':'1'};""")
    session.execute("""use word""")
    session.execute("create table if not exists word_count (word text ,count int, primary key(word))")
    prepared_stmt = session.prepare ( "INSERT INTO word_count (word,count) VALUES (?, ?)")
    print output
    for (word,count) in output:
        if word != '':
            bound_stmt = prepared_stmt.bind([word, count])
            stmt = session.execute(bound_stmt)
    res = json.dumps(dic,indent = 4)
    response = make_response(res, 200)
    return response

if __name__ == "__main__":
    app.run(host="0.0.0.0")



