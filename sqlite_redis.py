import redis
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go


def compute_mean_and_std(tab):
    mean = np.average(tab)
    std = np.std(tab)
    return mean, std

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
    #################################################
    ##################### REDIS #####################
    #################################################

    # Insertion de données dans redis

    tab_insert_redis = []
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        # Utilisation d'une donnée existant réellement dans la base de données
        r.set(i, "(2016, 'mental health survey for 2016', 25, 'Did your previous employers ever formally discuss mental health (as part of a wellness campaign or other official communication)?', 'None did', 2141)")
        end_time = datetime.now().timestamp()
        tab_insert_redis.append(end_time - start_time)

    # Récupération de données dans redis
    tab_retrieval_redis = []
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        r.get(i)
        end_time = datetime.now().timestamp()
        tab_retrieval_redis.append(end_time - start_time)

    # Mise à jour de données dans redis
    tab_update_redis = []
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        # Modification d'une donnée existant réellement dans la base de données
        r.set(i, "(2015, 'mental health survey for 2015', 999, 'Have you ever studied computer science?', 'None did', 2141)")
        end_time = datetime.now().timestamp()
        tab_update_redis.append(end_time - start_time)

    # Suppression de données dans redis
    tab_delete_redis = []
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        r.delete(i)
        end_time = datetime.now().timestamp()
        tab_delete_redis.append(end_time - start_time)

    ##################################################
    ##################### SQLITE #####################
    ##################################################
    
    # Insertion de données dans sqlite
    tab_first_insert_sqlite = []
    tab_second_insert_sqlite = []
    tab_total_insert_sqlite = []
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
        tab_first_insert_sqlite.append(intermediate_time - start_time)
        tab_second_insert_sqlite.append(end_time - intermediate_time)
        tab_total_insert_sqlite.append(((intermediate_time - start_time)+(end_time - intermediate_time)))

    # Récupération de données dans sqlite
    tab_retrieval_sqlite = []
    for i in range(nb_data+1, nb_data+1001):
        start_time = datetime.now().timestamp()
        ans = cur.execute("SELECT answer.SurveyID, survey.description, answer.QuestionID, question.questiontext, answer.AnswerText, answer.UserID from survey join answer on survey.SurveyId = answer.surveyId join question on question.QuestionID = answer.QuestionID and answer.QuestionID = " + str(i))
        end_time = datetime.now().timestamp()
        # print le select
        # for row in ans:
        #     print(row)
        tab_retrieval_sqlite.append(end_time - start_time)

    # Mise à jour de données dans sqlite
    tab_first_update_sqlite = []
    tab_second_update_sqlite = []
    tab_total_update_sqlite = []
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
        tab_first_update_sqlite.append(intermediate_time - start_time)
        tab_second_update_sqlite.append(end_time - intermediate_time)
        tab_total_update_sqlite.append(((intermediate_time - start_time)+(end_time - intermediate_time)))

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
    tab_first_delete_sqlite = []
    tab_second_delete_sqlite = []
    tab_total_delete_sqlite = []
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
        tab_first_delete_sqlite.append(intermediate_time - start_time)
        tab_second_delete_sqlite.append(end_time - intermediate_time)
        tab_total_delete_sqlite.append(((intermediate_time - start_time)+(end_time - intermediate_time)))


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

    r.flushall()

    compute_mean_and_std(tab_total_insert_sqlite)
    compute_mean_and_std(tab_retrieval_sqlite)
    compute_mean_and_std(tab_total_update_sqlite)
    compute_mean_and_std(tab_total_delete_sqlite)
    return [[tab_insert_redis, tab_retrieval_redis, tab_update_redis, tab_delete_redis], 
            [tab_total_insert_sqlite, tab_first_insert_sqlite, tab_second_insert_sqlite, tab_retrieval_sqlite,
            tab_total_update_sqlite, tab_first_update_sqlite, tab_second_insert_sqlite, tab_total_delete_sqlite,
            tab_first_delete_sqlite, tab_second_delete_sqlite]]
    

'''
tab un tableau
int un entier compris dans [0;3]: 0 = insert; 1 = retrieval; 2 = update; 3 = delete
'''
def print_and_return_redis_perfs(tab, i):
    mean, std = compute_mean_and_std(tab)
    if i == 0:
        print(f"Temps d'insertion en moyenne sur 1000 données sur Redis:             {mean:.8f} s. Écart-type : {std:.8f}")
    elif i==1:
        print(f"Temps de récupération en moyenne sur 1000 données sur Redis:             {mean:.8f} s. Écart-type : {std:.8f}")
    elif i==2:
        print(f"Temps de modification en moyenne sur 1000 données sur Redis:             {mean:.8f} s. Écart-type : {std:.8f}")
    elif i==3:
        print(f"Temps de suppression en moyenne sur 1000 données sur Redis:             {mean:.8f} s. Écart-type : {std:.8f}")
    return [mean, std]

def print_and_return_sqlite_perfs(tab_total, tab_first, tab_second, i):
    mean_total, std_total = compute_mean_and_std(tab_total)
    mean_first, std_first = compute_mean_and_std(tab_first)
    mean_second, std_second = compute_mean_and_std(tab_second)
    if i == 0:
        print(f"Temps d'insertion en moyenne sur 1000 données sur SQLite:             {mean_total:.8f} s. Écart-type : {std_total:.8f}")
        print(f"Temps d'insertion de la 1ère table en moyenne sur 1000 données sur SQLite: {mean_first:.8f} s. Écart-type : {std_first:.8f}")
        print(f"Temps d'insertion de la 2ème table en moyenne sur 1000 données sur SQLite:  {mean_second:.8f} s. Écart-type : {std_second:.8f}")
    elif i == 1:
        print(f"Temps de modification en moyenne sur 1000 données sur SQLite:             {mean_total:.8f} s. Écart-type : {std_total:.8f}")
        print(f" Temps de modification de la 1ere table en moyenne sur 1000 données sur SQLite: {mean_first:.8f} s. Écart-type : {std_first:.8f}")
        print(f" Temps de modification de la 2eme table en moyenne sur 1000 données sur SQLite:  {mean_second:.8f} s. Écart-type : {std_second:.8f}")
    elif i == 2:
        print(f"Temps de suppression en moyenne sur 1000 données sur SQLite:             {mean_total:.8f} s. Écart-type : {std_total:.8f}")
        print(f" Temps de suppression de la 1ere table en moyenne sur 1000 données sur SQLite:  {mean_first:.8f} s. Écart-type : {std_first:.8f}")
        print(f" Temps de suppression de la 2eme table en moyenne sur 1000 données sur SQLite:   {mean_second:.8f} s. Écart-type : {std_second:.8f}\n\n")
    return [mean_total, std_total]

def mean_std(tab, tab2, x):
    result = []
    if x == 0:
        for i in range(len(tab)):
            result.append(tab[i] + tab2[i])
    else:
        for i in range(len(tab)):
            result.append(tab[i] - tab2[i])
    return result

list_nb_data = [2000, 25000, 50000, 75000, 100000, 125000, 150000, 175000, 200000]

mean_redis_insert = []
sd_redis_insert = []
mean_redis_retrieval = []
sd_redis_retrieval = []
mean_redis_update = []
sd_redis_update = []
mean_redis_delete = []
sd_redis_delete = []

mean_sqlite_insert = []
sd_sqlite_insert = []
mean_sqlite_retrieval = []
sd_sqlite_retrieval = []
mean_sqlite_update = []
sd_sqlite_update = []
mean_sqlite_delete = []
sd_sqlite_delete = []

for data in list_nb_data:
    perfs = launch_all(data)
    mean, std = print_and_return_redis_perfs(perfs[0][0],0)
    mean_redis_insert.append(mean)
    sd_redis_insert.append(std)
    mean, std = print_and_return_redis_perfs(perfs[0][1],1)
    mean_redis_retrieval.append(mean)
    sd_redis_retrieval.append(std)
    mean, std = print_and_return_redis_perfs(perfs[0][2],2)
    mean_redis_update.append(mean)
    sd_redis_update.append(std)
    mean, std = print_and_return_redis_perfs(perfs[0][3],3)
    mean_redis_delete.append(mean)
    sd_redis_delete.append(std)

    
    mean, std = compute_mean_and_std(perfs[1][3])
    print(f"Temps de récupération en moyenne sur 1000 données sur SQLite:            {mean:.8f} s. Écart-type : {std:.8f}")
    mean_sqlite_retrieval.append(mean)
    sd_sqlite_retrieval.append(std)
    mean, std = print_and_return_sqlite_perfs(perfs[1][0],perfs[1][1],perfs[1][2],0)
    mean_sqlite_insert.append(mean)
    sd_sqlite_insert.append(std)
    mean, std = print_and_return_sqlite_perfs(perfs[1][4],perfs[1][5],perfs[1][6],1)
    mean_sqlite_update.append(mean)
    sd_sqlite_update.append(std)
    mean, std = print_and_return_sqlite_perfs(perfs[1][7],perfs[1][8],perfs[1][9],2)
    mean_sqlite_delete.append(mean)
    sd_sqlite_delete.append(std)

sum_meanstd_redis_insert = mean_std(mean_redis_insert,sd_redis_insert,0)
sum_meanstd_redis_retrieval = mean_std(mean_redis_retrieval,sd_redis_retrieval,0)
sum_meanstd_redis_update = mean_std(mean_redis_update,sd_redis_update,0)
sum_meanstd_redis_delete = mean_std(mean_redis_delete,sd_redis_delete,0)
sum_meanstd_redis_insert_neg = mean_std(mean_redis_insert,sd_redis_insert,1)
sum_meanstd_redis_retrieval_neg = mean_std(mean_redis_retrieval,sd_redis_retrieval,1)
sum_meanstd_redis_update_neg = mean_std(mean_redis_update,sd_redis_update,1)
sum_meanstd_redis_delete_neg = mean_std(mean_redis_delete,sd_redis_delete,1)

liste_l = ['nb_data','Insertion',"Selection","Mise a jour","Suppression",
           'Insertion_std_borne_sup',"Selection_std_borne_sup","Mise a jour_std_borne_sup","Suppression_std_borne_sup",
           'Insertion_std_borne_inf',"Selection_std_borne_inf","Mise a jour_std_borne_inf","Suppression_std_borne_inf"]

data_redis = {
    'nb_data': list_nb_data,
    'Insertion': mean_redis_insert,
    'Selection': mean_redis_retrieval,
    'Mise a jour': mean_redis_update,
    'Suppression': mean_redis_delete,
    'Insertion_std_borne_sup': sum_meanstd_redis_insert,
    'Selection_std_borne_sup': sum_meanstd_redis_retrieval,
    'Mise a jour_std_borne_sup': sum_meanstd_redis_update,
    'Suppression_std_borne_sup': sum_meanstd_redis_delete,
    'Insertion_std_borne_inf': sum_meanstd_redis_insert_neg,
    'Selection_std_borne_inf': sum_meanstd_redis_retrieval_neg,
    'Mise a jour_std_borne_inf': sum_meanstd_redis_update_neg,
    'Suppression_std_borne_inf': sum_meanstd_redis_delete_neg
}

df_redis = pd.DataFrame(data=data_redis)
fig = go.Figure()

fig.add_trace(go.Scatter(x=df_redis['nb_data'], y=df_redis['Insertion'],
                         line=dict(color='blue'), mode='lines', name='Insertion'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'], y=df_redis['Suppression'],
                         line=dict(color='red'), mode='lines', name='Suppression'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'], y=df_redis['Mise a jour'],
                         line=dict(color='green'), mode='lines', name='Mise a jour'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'], y=df_redis['Selection'],
                         line=dict(color='purple'), mode='lines', name='Récupération'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Insertion_std_borne_sup'].tolist() + df_redis['Insertion_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,100,80,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Insertion Intervalle'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Suppression_std_borne_sup'].tolist() + df_redis['Suppression_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(255,0,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Suppression Intervalle'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Mise a jour_std_borne_sup'].tolist() + df_redis['Mise a jour_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,255,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Mise a jour Intervalle'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Selection_std_borne_sup'].tolist() + df_redis['Selection_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(128,0,128,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Récupération Intervalle'))

fig.update_layout(
    title='Performance evolution with redis',
    xaxis_title='nb_data',
    yaxis_title='Values',
)


pio.write_html(fig, file='graphe_redis.html', auto_open=True)

sum_meanstd_sqlite_insert = mean_std(mean_sqlite_insert,sd_sqlite_insert,0)
sum_meanstd_sqlite_retrieval = mean_std(mean_sqlite_retrieval,sd_sqlite_retrieval,0)
sum_meanstd_sqlite_update = mean_std(mean_sqlite_update,sd_sqlite_update,0)
sum_meanstd_sqlite_delete = mean_std(mean_sqlite_delete,sd_sqlite_delete,0)
sum_meanstd_sqlite_insert_neg = mean_std(mean_sqlite_insert,sd_sqlite_insert,1)
sum_meanstd_sqlite_retrieval_neg = mean_std(mean_sqlite_retrieval,sd_sqlite_retrieval,1)
sum_meanstd_sqlite_update_neg = mean_std(mean_sqlite_update,sd_sqlite_update,1)
sum_meanstd_sqlite_delete_neg = mean_std(mean_sqlite_delete,sd_sqlite_delete,1)

data_sqlite = {
    'nb_data': list_nb_data,
    'Insertion': mean_sqlite_insert, 
    'Selection': mean_sqlite_retrieval, 
    'Mise a jour': mean_sqlite_update, 
    'Suppression': mean_sqlite_delete,
    'Insertion_std_borne_sup': sum_meanstd_sqlite_insert,
    'Selection_std_borne_sup': sum_meanstd_sqlite_retrieval,
    'Mise a jour_std_borne_sup': sum_meanstd_sqlite_update,
    'Suppression_std_borne_sup': sum_meanstd_sqlite_delete,
    'Insertion_std_borne_inf': sum_meanstd_sqlite_insert_neg,
    'Selection_std_borne_inf': sum_meanstd_sqlite_retrieval_neg,
    'Mise a jour_std_borne_inf': sum_meanstd_sqlite_update_neg,
    'Suppression_std_borne_inf': sum_meanstd_sqlite_delete_neg
}


df_sqlite = pd.DataFrame(data=data_sqlite)
fig = go.Figure()

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'], y=df_sqlite['Insertion'],
                         line=dict(color='blue'), mode='lines', name='Insertion'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'], y=df_sqlite['Suppression'],
                         line=dict(color='red'), mode='lines', name='Suppression'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'], y=df_sqlite['Mise a jour'],
                         line=dict(color='green'), mode='lines', name='Mise a jour'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'], y=df_sqlite['Selection'],
                         line=dict(color='purple'), mode='lines', name='Récupération'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Insertion_std_borne_sup'].tolist() + df_sqlite['Insertion_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,100,80,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Insertion Intervalle'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Suppression_std_borne_sup'].tolist() + df_sqlite['Suppression_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(255,0,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Suppression Intervalle'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Mise a jour_std_borne_sup'].tolist() + df_sqlite['Mise a jour_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,255,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Mise a jour Intervalle'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Selection_std_borne_sup'].tolist() + df_sqlite['Selection_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(128,0,128,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Récupération Intervalle'))

fig.update_layout(
    title='Performance evolution with SQLite',
    xaxis_title='nb_data',
    yaxis_title='Values',
)

pio.write_html(fig, file='graphe_sqlite.html', auto_open=True)