import redis
import sqlite3
import plotly.express as px
import pandas as pd
from datetime import datetime


def launch_all(nb_data):
    print("##################################################")
    print("BASE AVEC",nb_data,"DONNEES ")
    print("##################################################\n\n")
    start_time = datetime.now().timestamp()
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    end_time = datetime.now().timestamp()
    print("Temps de connexion à la base de données redis:\n" + str(end_time - start_time) + " secondes.")

    # ##################### IMPORT DES DONNEES #####################

    # Import des données sqlite dans redis
    start_time = datetime.now().timestamp()
    con_init = sqlite3.connect("mental_health.sqlite")
    end_time = datetime.now().timestamp()
    print("Temps de connexion à la base de données sqlite:\n" + str(end_time - start_time) + " secondes.")
    cur_init = con_init.cursor() 
    res = cur_init.execute("SELECT answer.SurveyID, survey.description, answer.QuestionID, question.questiontext, answer.AnswerText, answer.UserID from survey join answer on survey.SurveyId = answer.surveyId join question on question.QuestionID = answer.QuestionID LIMIT " + str(nb_data))
    i = 0
    for row in res:
        r.set(str(i),str(row))
        i = i+1


    # Import des données sqlite dans une nouvelle base sqlite
    con = sqlite3.connect("mental_health_"+str(nb_data)+".sqlite")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS survey (SurveyID INTEGER PRIMARY KEY, description TEXT);")
    cur.execute("CREATE TABLE IF NOT EXISTS question (questiontext TEXT, QuestionID INTEGER PRIMARY KEY);")
    cur.execute("CREATE TABLE IF NOT EXISTS answer (AnswerText TEXT, SurveyID INTEGER, UserID INTEGER, QuestionID INTEGER, PRIMARY KEY(SurveyID, UserID, QuestionID));")
    con.commit()

    res = cur_init.execute("SELECT * from survey ;")
    result = res.fetchall()
    res2 = cur_init.execute("SELECT * from question ;")
    result2 = res2.fetchall()
    res3 = cur_init.execute("SELECT * from answer LIMIT " + str(nb_data)+";")
    result3 = res3.fetchall()

    #print(result)

    for res in result:
        cur.execute("INSERT INTO survey (SurveyID, description) VALUES (?, ?)", res)

    for res in result2:
        cur.execute("INSERT INTO question (questiontext, QuestionID) VALUES (?, ?)", res)

    for res in result3:
        cur.execute("INSERT INTO answer (AnswerText, SurveyID, UserID, QuestionID) VALUES (?, ?, ?, ?)", res)

    con.commit()
    ##################### REDIS #####################

    # Insertion de données dans redis
    total_time_insertion_redis = 0
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        # Utilisation d'une donnée existant réellement dans la base de données
        r.set(i, "(2016, 'mental health survey for 2016', 25, 'Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?', 'None did', 2141)")
        end_time = datetime.now().timestamp()
        total_time_insertion_redis = total_time_insertion_redis + (end_time - start_time)

    total_time_insertion_redis /= 1000

    # Récupération de données dans redis
    total_time_retrieval_redis = 0
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        r.get(i)
        end_time = datetime.now().timestamp()
        total_time_retrieval_redis = total_time_retrieval_redis + (end_time - start_time)

    total_time_retrieval_redis /= 1000

    # Mise à jour de données dans redis
    total_time_modification_redis = 0
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        # Modification d'une donnée existant réellement dans la base de données
        r.set(i, "(2015, 'mental health survey for 2015', 999, 'Have you ever studied computer science?', 'None did', 2141)")
        end_time = datetime.now().timestamp()
        total_time_modification_redis = total_time_modification_redis + (end_time - start_time)

    total_time_modification_redis /= 1000

    # Suppression de données dans redis
    total_time_deletion_redis = 0
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        r.delete(i)
        end_time = datetime.now().timestamp()
        total_time_deletion_redis = total_time_deletion_redis + (end_time - start_time)

    total_time_deletion_redis /= 1000

    ##################### SQLITE #####################

    # Insertion de données dans sqlite
    time_first_insertion_sqlite = 0
    time_second_insertion_sqlite = 0
    total_time_insertion_sqlite = 0
    for i in range(nb_data+1, nb_data+1001):
        query_question = "INSERT INTO question (questiontext, QuestionID) VALUES ('Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?', '" + str(i) + "');"
        query_answer = "INSERT INTO answer (AnswerText, SurveyID, UserID, QuestionID) VALUES ('None did', 2016, 2141, '" + str(i) + "');"
        start_time = datetime.now().timestamp()
        cur.execute(query_question)
        con.commit()
        intermediate_time = datetime.now().timestamp()
        cur.execute(query_answer)
        con.commit()
        end_time = datetime.now().timestamp()
        time_first_insertion_sqlite = time_first_insertion_sqlite + (intermediate_time - start_time)
        time_second_insertion_sqlite = time_second_insertion_sqlite + (end_time - intermediate_time)
        total_time_insertion_sqlite = total_time_insertion_sqlite + (end_time - start_time)

    time_first_insertion_sqlite /= 1000
    time_second_insertion_sqlite /= 1000
    total_time_insertion_sqlite /= 1000

    # Récupération de données dans sqlite
    total_time_retrieval_sqlite = 0
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        ans = cur.execute("SELECT answer.SurveyID, survey.description, answer.QuestionID, question.questiontext, answer.AnswerText, answer.UserID from survey join answer on survey.SurveyId = answer.surveyId join question on question.QuestionID = answer.QuestionID and answer.QuestionID = " + str(i))
        end_time = datetime.now().timestamp()
        # print le select
        # for row in ans:
        #     print(row)
        total_time_retrieval_sqlite = total_time_retrieval_sqlite + (end_time - start_time)

    total_time_retrieval_sqlite /= 1000

    # Mise à jour de données dans sqlite
    total_time_modification_sqlite = 0
    time_first_modification_sqlite = 0
    time_second_modification_sqlite = 0
    for i in range(nb_data+1, nb_data+1001):
        query_question = "UPDATE question SET questiontext = 'Have you ever studied computer science?', QuestionID = " + str(i+1001) + " WHERE questiontext = 'Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?' and QuestionID = " + str(i) + ";"
        query_answer = "UPDATE answer SET SurveyID = 2015, QuestionID = "+ str(i+1001) + " WHERE SurveyID = 2016 AND UserID = 2141 AND AnswerText = 'None did' AND QuestionID = " + str(i) + ";"
        start_time = datetime.now().timestamp()
        cur.execute(query_question)
        con.commit()
        intermediate_time = datetime.now().timestamp()
        cur.execute(query_answer)
        con.commit()
        end_time = datetime.now().timestamp()
        time_first_modification_sqlite = time_first_modification_sqlite + (intermediate_time - start_time)
        time_second_modification_sqlite = time_second_modification_sqlite + (end_time - intermediate_time)
        total_time_modification_sqlite = total_time_modification_sqlite + (end_time - start_time)

    time_first_modification_sqlite /= 1000
    time_second_modification_sqlite /= 1000
    total_time_modification_sqlite /= 1000

    # On vérifie si les données ont bien été modifées dans sqlite

    # print("Modification données")
    # ans = cur.execute("SELECT * from answer where QuestionID > 236898")
    # for row in ans:
    #     print(row)
    # ans = cur.execute("SELECT COUNT(*) from answer where QuestionID > 236898")
    # for row in ans:
    #     print(row)
    # # Verify that such data does not exist anymore
    # ans = cur.execute("SELECT * from answer where QuestionID > 236898")
    # for row in ans:
    #     print(row)

    # Suppression de données dans sqlite
    time_first_deletion_sqlite = 0
    time_second_deletion_sqlite = 0
    total_time_deletion_sqlite = 0
    for i in range(nb_data+1, nb_data+1001):
        query_delete_question = ("DELETE FROM question where QuestionID = " + str(i+1001))
        query_delete_answer = ("DELETE FROM answer where QuestionID = " + str(i+1001))
        start_time = datetime.now().timestamp()
        cur.execute(query_delete_question)
        con.commit()
        intermediate_time = datetime.now().timestamp()
        cur.execute(query_delete_answer)
        con.commit()
        end_time = datetime.now().timestamp()
        time_first_deletion_sqlite = time_first_deletion_sqlite + (intermediate_time - start_time)
        time_second_deletion_sqlite = time_second_deletion_sqlite + (end_time - intermediate_time)
        total_time_deletion_sqlite = total_time_deletion_sqlite + (end_time - start_time)
        
    time_first_deletion_sqlite /= 1000
    time_second_deletion_sqlite /= 1000
    total_time_deletion_sqlite /= 1000


    # On vérifie si les données ont bien été supprimées dans sqlite

    # print("Suppression données")
    # ans = cur.execute("SELECT * from answer where QuestionID > 236898")
    # for row in ans:
    #     print(row)
    # ans = cur.execute("SELECT COUNT(*) from answer where QuestionID > 236898")
    # for row in ans:
    #     print(row)
    # # Verify that such data does not exist anymore
    # ans = cur.execute("SELECT * from answer where QuestionID > 236898")
    # for row in ans:
    #     print(row)

    con_init.close()
    con.close()

    print(f"Temps d'insertion en moyenne sur mille données sur Redis:                 {total_time_insertion_redis:.8f} secondes.")
    print(f"Temps de récupération en moyenne sur mille données sur Redis:             {total_time_retrieval_redis:.8f} secondes.")
    print(f"Temps de modification en moyenne sur mille données sur Redis:             {total_time_modification_redis:.8f} secondes.")
    print(f"Temps de suppression en moyenne sur mille données sur Redis:              {total_time_deletion_redis:.8f} secondes.")

    print(f"Temps d'insertion en moyenne sur mille données sur SQLite:                {total_time_insertion_sqlite:.8f} secondes.")
    print(f"--- Temps d'insertion de la première table en moyenne sur mille données sur SQLite: {time_first_insertion_sqlite:.8f} secondes.")
    print(f"--- Temps d'insertion de la seconde table en moyenne sur mille données sur SQLite:  {time_second_insertion_sqlite:.8f} secondes.")
    print(f"Temps de récupération en moyenne sur mille données sur SQLite:            {total_time_retrieval_sqlite:.8f} secondes.")
    print(f"Temps de modification en moyenne sur mille données sur SQLite:            {total_time_modification_sqlite:.8f} secondes.")
    print(f"--- Temps de modification de la première table en moyenne sur mille données sur SQLite: {time_first_modification_sqlite:.8f} secondes.")
    print(f"--- Temps de modification de la seconde table en moyenne sur mille données sur SQLite:  {time_second_modification_sqlite:.8f} secondes.")
    print(f"Temps de suppression en moyenne sur mille données sur SQLite:             {total_time_deletion_sqlite:.8f} secondes.")
    print(f"--- Temps de suppression de la première table en moyenne sur mille données sur SQLite:  {time_first_deletion_sqlite:.8f} secondes.")
    print(f"--- Temps de suppression de la seconde table en moyenne sur mille données sur SQLite:   {time_second_deletion_sqlite:.8f} secondes.\n\n")




    r.flushall()

    return [[total_time_insertion_redis, total_time_retrieval_redis, total_time_modification_redis, total_time_deletion_redis], 
            [total_time_insertion_sqlite, time_first_insertion_sqlite, time_second_insertion_sqlite, total_time_retrieval_sqlite,
            total_time_modification_sqlite, time_first_modification_sqlite, time_second_modification_sqlite, total_time_deletion_sqlite,
            time_first_deletion_sqlite, time_second_deletion_sqlite]]




list_nb_data = [2000, 25000, 50000, 75000, 100000, 125000, 150000, 175000, 200000]
redis_perfs = []
sqlite_perfs = []
for data in list_nb_data:
    perfs = launch_all(data)
    redis_perfs.append(perfs[0])
    sqlite_perfs.append(perfs[1])

d_redis = {
    'nb_data': list_nb_data,
    'Insertion': [redis_perfs[0][0], redis_perfs[1][0], redis_perfs[2][0], redis_perfs[3][0], redis_perfs[4][0], redis_perfs[5][0], redis_perfs[6][0], redis_perfs[7][0], redis_perfs[8][0]], 
    'Selection': [redis_perfs[0][1],  redis_perfs[1][1], redis_perfs[2][1], redis_perfs[3][1], redis_perfs[4][1], redis_perfs[5][1], redis_perfs[6][1], redis_perfs[7][1], redis_perfs[8][1]], 
    'Mise à jour': [redis_perfs[0][2],  redis_perfs[1][2], redis_perfs[2][2], redis_perfs[3][2], redis_perfs[4][2], redis_perfs[5][2], redis_perfs[6][2], redis_perfs[7][2], redis_perfs[8][2]], 
    'Suppression': [redis_perfs[0][3], redis_perfs[1][3], redis_perfs[2][3], redis_perfs[3][3], redis_perfs[4][3], redis_perfs[5][3], redis_perfs[6][3], redis_perfs[7][3], redis_perfs[8][3]]
}

df_redis = pd.DataFrame(data=d_redis)


fig = px.line(df_redis, x='nb_data', y=['Insertion', 'Selection', 'Mise à jour', 'Suppression'], title='Performance evolution with redis')
fig.show()

d_sqlite = {
    'nb_data': list_nb_data,
    'Insertion': [sqlite_perfs[0][0], sqlite_perfs[1][0], sqlite_perfs[2][0], sqlite_perfs[3][0], sqlite_perfs[4][0], sqlite_perfs[5][0], sqlite_perfs[6][0], sqlite_perfs[7][0], sqlite_perfs[8][0]], 
    'Selection': [sqlite_perfs[0][3],  sqlite_perfs[1][3], sqlite_perfs[2][3], sqlite_perfs[3][3], sqlite_perfs[4][3], sqlite_perfs[5][3], sqlite_perfs[6][3], sqlite_perfs[7][3], sqlite_perfs[8][3]], 
    'Mise à jour': [sqlite_perfs[0][4],  sqlite_perfs[1][4], sqlite_perfs[2][4], sqlite_perfs[3][4], sqlite_perfs[4][4], sqlite_perfs[5][4], sqlite_perfs[6][4], sqlite_perfs[7][4], sqlite_perfs[8][4]], 
    'Suppression': [sqlite_perfs[0][7], sqlite_perfs[1][7], sqlite_perfs[2][7], sqlite_perfs[3][7], sqlite_perfs[4][7], sqlite_perfs[5][7], sqlite_perfs[6][7], sqlite_perfs[7][7], sqlite_perfs[8][7]]
}

df_sqlite = pd.DataFrame(data=d_sqlite)


fig = px.line(df_sqlite, x='nb_data', y=['Insertion', 'Selection', 'Mise à jour', 'Suppression'], title='Performance evolution with sqlite')
fig.show()
