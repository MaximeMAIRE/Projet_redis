import redis
import sqlite3
from datetime import datetime


def launch_all(nb_data):
    start_time = datetime.now().timestamp()
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    end_time = datetime.now().timestamp()
    print("Temps de connexion à la base de données redis:\n" + str(end_time - start_time) + " secondes.")

    # ##################### IMPORT DES DONNEES #####################

    # # Import des données sqlite dans redis
    # start_time = datetime.now().timestamp()
    con_init = sqlite3.connect("mental_health.sqlite")
    # end_time = datetime.now().timestamp()
    # print("Temps de connexion à la base de données sqlite:\n" + str(end_time - start_time) + " secondes.")
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

    print(result)

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
    for i in range(236899, 237000):
        start_time = datetime.now().timestamp()
        # Utilisation d'une donnée existant réellement dans la base de données
        r.set(i, "(2016, 'mental health survey for 2016', 25, 'Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?', 'None did', 2141)")
        end_time = datetime.now().timestamp()
        total_time_insertion_redis = total_time_insertion_redis + (end_time - start_time)

    total_time_insertion_redis /= 100

    # Récupération de données dans redis
    total_time_retrieval_redis = 0
    for i in range(236899, 237000):
        start_time = datetime.now().timestamp()
        r.get(i)
        end_time = datetime.now().timestamp()
        total_time_retrieval_redis = total_time_retrieval_redis + (end_time - start_time)

    total_time_retrieval_redis /= 100

    # Mise à jour de données dans redis
    total_time_modification_redis = 0
    for i in range(236899, 237000):
        start_time = datetime.now().timestamp()
        # Modification d'une donnée existant réellement dans la base de données
        r.set(i, "(2015, 'mental health survey for 2015', 999, 'Have you ever studied computer science?', 'None did', 2141)")
        end_time = datetime.now().timestamp()
        total_time_modification_redis = total_time_modification_redis + (end_time - start_time)

    total_time_modification_redis /= 100

    # Suppression de données dans redis
    total_time_deletion_redis = 0
    for i in range(236899, 237000):
        start_time = datetime.now().timestamp()
        r.delete(i)
        end_time = datetime.now().timestamp()
        total_time_deletion_redis = total_time_deletion_redis + (end_time - start_time)

    total_time_deletion_redis /= 100

    ##################### SQLITE #####################

    # Insertion de données dans sqlite
    time_first_insertion_sqlite = 0
    time_second_insertion_sqlite = 0
    total_time_insertion_sqlite = 0
    for i in range(236899, 237000):
        query_question = "INSERT INTO question VALUES ('Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?', '" + str(i) + "');"
        query_answer = "INSERT INTO answer VALUES ('None did', 2016, 2141, '" + str(i) + "');"

        start_time = datetime.now().timestamp()
        cur = con.cursor()
        cur.execute(query_question)
        intermediate_time = datetime.now().timestamp()
        cur.execute(query_answer)
        con.commit()
        end_time = datetime.now().timestamp()
        time_first_insertion_sqlite = time_first_insertion_sqlite + (intermediate_time - start_time)
        time_second_insertion_sqlite = time_second_insertion_sqlite + (end_time - intermediate_time)
        total_time_insertion_sqlite = total_time_insertion_sqlite + (end_time - start_time)

    time_first_insertion_sqlite /= 100
    time_second_insertion_sqlite /= 100
    total_time_insertion_sqlite /= 100

    # Récupération de données dans sqlite
    total_time_retrieval_sqlite = 0
    for i in range(236899, 237000):
        start_time = datetime.now().timestamp()
        #cur = con.cursor()
        ans = cur.execute("SELECT answer.SurveyID, survey.description, answer.QuestionID, question.questiontext, answer.AnswerText, answer.UserID from survey join answer on survey.SurveyId = answer.surveyId join question on question.QuestionID = answer.QuestionID and answer.QuestionID = " + str(i))
        end_time = datetime.now().timestamp()
        for row in ans:
            print(row)
        total_time_retrieval_sqlite = total_time_retrieval_sqlite + (end_time - start_time)

    total_time_retrieval_sqlite /= 100

    # Mise à jour de données dans sqlite
    total_time_modification_sqlite = 0
    time_first_modification_sqlite = 0
    time_second_modification_sqlite = 0
    j = 1
    for i in range(236899, 237000):
        query_question = "UPDATE question SET questiontext = 'Have you ever studied computer science?', QuestionID = " + str(i+j) + " WHERE questiontext = 'Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?' and QuestionID = " + str(i) + ";"
        query_answer = "UPDATE answer SET SurveyID = 2015, QuestionID = "+ str(i+j) + " WHERE SurveyID = 2016 AND UserID = 2141 AND AnswerText = 'None did' AND QuestionID = " + str(i) + ";"
        cur = con.cursor()
        start_time = datetime.now().timestamp()
        cur.execute(query_question)
        intermediate_time = datetime.now().timestamp()
        cur.execute(query_answer)
        con.commit()    
        end_time = datetime.now().timestamp()
        for row in ans:
            print(row)
        time_first_modification_sqlite = time_first_modification_sqlite + (intermediate_time - start_time)
        time_second_modification_sqlite = time_second_modification_sqlite + (end_time - intermediate_time)
        total_time_modification_sqlite = total_time_modification_sqlite + (end_time - start_time)
        j+=1

    time_first_modification_sqlite /= 100
    time_second_modification_sqlite /= 100
    total_time_modification_sqlite /= 100

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
    j=1
    for i in range(236899, 237000):
        query_delete_question = ("DELETE FROM question where QuestionID = " + str(i+j))
        query_delete_answer = ("DELETE FROM answer where QuestionID = " + str(i+j))
        start_time = datetime.now().timestamp()
        cur.execute(query_delete_question)
        intermediate_time = datetime.now().timestamp()
        cur.execute(query_delete_answer)
        con.commit()
        end_time = datetime.now().timestamp()
        time_first_deletion_sqlite = time_first_deletion_sqlite + (intermediate_time - start_time)
        time_second_deletion_sqlite = time_second_deletion_sqlite + (end_time - intermediate_time)
        total_time_deletion_sqlite = total_time_deletion_sqlite + (end_time - start_time)
        j+=1

    time_first_deletion_sqlite /= 100
    time_second_deletion_sqlite /= 100
    total_time_deletion_sqlite /= 100


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

    con.close()

    print("Temps d'insertion en moyenne sur cent données sur redis:\n" + str(total_time_insertion_redis) + " secondes.")
    print("\nTemps de récupération en moyenne sur cent données sur redis:\n" + str(total_time_retrieval_redis) + " secondes.")
    print("\nTemps de modification en moyenne sur cent données sur redis:\n" + str(total_time_modification_redis) + " secondes.")
    print("\nTemps de suppression en moyenne sur cent données sur redis:\n" + str(total_time_deletion_redis) + " secondes.")


    print("\nTemps d'insertion en moyenne sur cent données sur sqlite:\n" + str(total_time_insertion_sqlite) + " secondes.")
    print("--- Temps d'insertion de la première table en moyenne sur cent données sur sqlite:\n" + str(time_first_insertion_sqlite) + " secondes.")
    print("--- Temps d'insertion de la seconde table en moyenne sur cent données sur sqlite:\n" + str(time_second_insertion_sqlite) + " secondes.")
    print("\nTemps de récupération en moyenne sur cent données sur sqlite:\n" + str(total_time_retrieval_sqlite) + " secondes.")
    print("\nTemps de modification en moyenne sur cent données sur sqlite:\n" + str(total_time_modification_sqlite) + " secondes.")
    print("--- Temps de modification de la première table en moyenne sur cent données sur sqlite:\n" + str(time_first_modification_sqlite) + " secondes.")
    print("--- Temps de modification de la seconde table en moyenne sur cent données sur sqlite:\n" + str(time_second_modification_sqlite) + " secondes.")
    print("\nTemps de suppression en moyenne sur cent données sur sqlite:\n" + str(total_time_deletion_sqlite) + " secondes.")
    print("--- Temps de suppression de la première table en moyenne sur cent données sur sqlite:\n" + str(time_first_deletion_sqlite) + " secondes.")
    print("--- Temps de suppression de la seconde table en moyenne sur cent données sur sqlite:\n" + str(time_second_deletion_sqlite) + " secondes.")

    r.flushall()

    return [[total_time_insertion_redis, total_time_retrieval_redis, total_time_modification_redis, total_time_deletion_redis], 
            [total_time_insertion_sqlite, time_first_insertion_sqlite, time_second_insertion_sqlite, total_time_retrieval_sqlite,
            total_time_modification_sqlite, time_first_modification_sqlite, time_second_modification_sqlite, total_time_deletion_sqlite,
            time_first_deletion_sqlite, time_second_deletion_sqlite]]




list_nb_data = [100, 1000, 10000, 50000, 100000, 200000]
redis_perfs = []
sqlite_perfs = []
for data in list_nb_data:
    perfs = launch_all(data)
    redis_perfs.append(perfs[0])
    sqlite_perfs.append(perfs[1])

