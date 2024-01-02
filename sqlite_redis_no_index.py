import redis
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go


def compute_mean_and_std(performance_list):
    """Returns the mean and the standard deviation of a list of trials for one operation (insertion, deletion, ...)

    Args:
        performance_list (float[]): The list of performances for one operation

    Returns:
        float, float: The mean and the standard deviation of the list of performances
    """
    mean = np.average(performance_list)
    std = np.std(performance_list)
    return mean, std

def launch_all(nb_data):
    print("##################################################")
    print("BASE CONTENANT",nb_data,"DONNEES ")
    print("##################################################\n\n")
    start_time = datetime.now().timestamp()
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    end_time = datetime.now().timestamp()
    print("Temps de connexion à la base de données redis:\n" + str(end_time - start_time) + " secondes.")

    ##################### IMPORT DES DONNEES #####################

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
    cur.execute("CREATE TABLE IF NOT EXISTS survey (SurveyID INTEGER, description TEXT);")
    cur.execute("CREATE TABLE IF NOT EXISTS question (questiontext TEXT, QuestionID INTEGER);")
    cur.execute("CREATE TABLE IF NOT EXISTS answer (AnswerText TEXT, SurveyID INTEGER, UserID INTEGER, QuestionID INTEGER);")
    con.commit()

    res = cur_init.execute("SELECT * from survey ;")
    result = res.fetchall()
    res2 = cur_init.execute("SELECT * from question ;")
    result2 = res2.fetchall()
    res3 = cur_init.execute("SELECT * from answer LIMIT " + str(nb_data)+";")
    result3 = res3.fetchall()

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

    return [[tab_insert_redis, tab_retrieval_redis, tab_update_redis, tab_delete_redis], 
            [tab_total_insert_sqlite, tab_first_insert_sqlite, tab_second_insert_sqlite, tab_retrieval_sqlite,
            tab_total_update_sqlite, tab_first_update_sqlite, tab_second_insert_sqlite, tab_total_delete_sqlite,
            tab_first_delete_sqlite, tab_second_delete_sqlite]]
    

def print_and_return_redis_perfs(performance_list, i):
    """Print and return redis mean and standard deviation for an operation

    Args:
        performance_list (float[]): List containing all the performance (time taken) for each trial on one operation
        i (int): Describes the type of operation: 1 = insertion, 2 = retrieval, 3 = update, 4 (or else) = delete

    Returns:
        float, float: The mean of the times taken for each operation and the standard deviation of these times
    """

    mean, std = compute_mean_and_std(performance_list)
    operation = ""

    if i == 0:
        operation = "d'insertion"
    elif i==1:
        operation = "de récupération"
    elif i==2:
        operation = "de modification"
    else:
        operation = "de suppression"

    print(f"Temps {operation} en moyenne sur 1000 données sur Redis:             {mean:.8f} s. Écart-type : {std:.8f}")

    return [mean, std]


def print_and_return_sqlite_perfs(tab_total, tab_first, tab_second, i):
    """Print and return SQLite mean and standard deviation for an operation

    Args:
        tab_total (float[]): List containing all the performance (total time taken = time taken on first and second tab) for each trial on one operation
        tab_first (float[]): List containing all the performance (time taken on the first tab) for each trial on one operation
        tab_second (float[]): List containing all the performance (time taken on the second) for each trial on one operation
        i (int): Describes the type of operation: 1 = insertion, 2 = retrieval, 3 = update, 4 (or else) = delete

    Returns:
        float, float: The mean of the times taken for each operation and the standard deviation of these times (on the two tables)
    """

    mean_total, std_total = compute_mean_and_std(tab_total)
    mean_first, std_first = compute_mean_and_std(tab_first)
    mean_second, std_second = compute_mean_and_std(tab_second)

    operation = ""
    new_line = ""
    if i == 0:
        operation = "d'insertion"
    elif i == 1:
        operation = "de modification"
    else:
        operation = "de suppression"
        new_line = "\n\n"

    print(f"Temps {operation} en moyenne sur 1000 données sur SQLite:             {mean_total:.8f} s. Écart-type : {std_total:.8f}")
    print(f"Temps {operation} de la 1ère table en moyenne sur 1000 données sur SQLite: {mean_first:.8f} s. Écart-type : {std_first:.8f}")
    print(f"Temps {operation} de la 2ème table en moyenne sur 1000 données sur SQLite:  {mean_second:.8f} s. Écart-type : {std_second:.8f} {new_line}")
    
    return [mean_total, std_total]


def compute_uncertainty_curve(mean_tab, std_tab):
    """For each operation (insertion, deletion, ...), we have a curve that represents
    the mean time taken for these operations using a different nb_data.
    We also have standard deviation values representing the uncertainty around the different means.
    This function builds the uncertainty curve around the the mean curve.

    Args:
        mean_tab (float[]): The different means ( = mean curve) of an operation
        std_tab (float[]): The different standard deviation of an operation

    Returns:
       float, float: The two uncertainty curves
    """

    # Uncertainty curve "above" the mean curve
    uncertainty_curve_pos = []
    # Uncertainty curve "under" the mean curve
    uncertainty_curve_neg = []

    for i in range(len(mean_tab)):
        uncertainty_curve_pos.append(mean_tab[i] + std_tab[i])
    for i in range(len(mean_tab)):
        uncertainty_curve_neg.append(mean_tab[i] - std_tab[i])

    return [uncertainty_curve_pos, uncertainty_curve_neg]

list_nb_data = [2000, 25000, 50000, 75000, 100000, 125000, 150000, 175000, 200000]

mean_redis_insert = []
std_redis_insert = []
mean_redis_retrieval = []
std_redis_retrieval = []
mean_redis_update = []
std_redis_update = []
mean_redis_delete = []
std_redis_delete = []

mean_sqlite_insert = []
std_sqlite_insert = []
mean_sqlite_retrieval = []
std_sqlite_retrieval = []
mean_sqlite_update = []
std_sqlite_update = []
mean_sqlite_delete = []
std_sqlite_delete = []

for data in list_nb_data:
    perfs = launch_all(data)
    mean, std = print_and_return_redis_perfs(perfs[0][0],0)
    mean_redis_insert.append(mean)
    std_redis_insert.append(std)
    mean, std = print_and_return_redis_perfs(perfs[0][1],1)
    mean_redis_retrieval.append(mean)
    std_redis_retrieval.append(std)
    mean, std = print_and_return_redis_perfs(perfs[0][2],2)
    mean_redis_update.append(mean)
    std_redis_update.append(std)
    mean, std = print_and_return_redis_perfs(perfs[0][3],3)
    mean_redis_delete.append(mean)
    std_redis_delete.append(std)

    
    mean, std = compute_mean_and_std(perfs[1][3])
    print(f"Temps de récupération en moyenne sur 1000 données sur SQLite:            {mean:.8f} s. Écart-type : {std:.8f}")
    mean_sqlite_retrieval.append(mean)
    std_sqlite_retrieval.append(std)
    mean, std = print_and_return_sqlite_perfs(perfs[1][0],perfs[1][1],perfs[1][2],0)
    mean_sqlite_insert.append(mean)
    std_sqlite_insert.append(std)
    mean, std = print_and_return_sqlite_perfs(perfs[1][4],perfs[1][5],perfs[1][6],1)
    mean_sqlite_update.append(mean)
    std_sqlite_update.append(std)
    mean, std = print_and_return_sqlite_perfs(perfs[1][7],perfs[1][8],perfs[1][9],2)
    mean_sqlite_delete.append(mean)
    std_sqlite_delete.append(std)

# Uncertainty curves for each operation in redis
redis_insert_uncertainty_curve_pos, redis_insert_uncertainty_curve_neg = compute_uncertainty_curve(mean_redis_insert,std_redis_insert)
redis_retrieval_uncertainty_curve_pos, redis_retrieval_uncertainty_curve_neg = compute_uncertainty_curve(mean_redis_retrieval,std_redis_retrieval)
redis_update_uncertainty_curve_pos, redis_update_uncertainty_curve_neg = compute_uncertainty_curve(mean_redis_update,std_redis_update)
redis_delete_uncertainty_curve_pos, redis_delete_uncertainty_curve_neg = compute_uncertainty_curve(mean_redis_delete,std_redis_delete)

data_redis = {
    'nb_data': list_nb_data,
    'Insertion': mean_redis_insert,
    'Selection': mean_redis_retrieval,
    'Mise a jour': mean_redis_update,
    'Suppression': mean_redis_delete,
    'Insertion_std_borne_sup': redis_insert_uncertainty_curve_pos,
    'Selection_std_borne_sup': redis_retrieval_uncertainty_curve_pos,
    'Mise a jour_std_borne_sup': redis_update_uncertainty_curve_pos,
    'Suppression_std_borne_sup': redis_delete_uncertainty_curve_pos,
    'Insertion_std_borne_inf': redis_insert_uncertainty_curve_neg,
    'Selection_std_borne_inf': redis_retrieval_uncertainty_curve_neg,
    'Mise a jour_std_borne_inf': redis_update_uncertainty_curve_neg,
    'Suppression_std_borne_inf': redis_delete_uncertainty_curve_neg
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
                         line=dict(color='rgba(255,255,255,0)'), name='Insertion - incertitude'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Suppression_std_borne_sup'].tolist() + df_redis['Suppression_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(255,0,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Suppression - incertitude'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Mise a jour_std_borne_sup'].tolist() + df_redis['Mise a jour_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,255,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Mise a jour - incertitude'))

fig.add_trace(go.Scatter(x=df_redis['nb_data'].tolist() + df_redis['nb_data'].tolist()[::-1],
                         y=df_redis['Selection_std_borne_sup'].tolist() + df_redis['Selection_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(128,0,128,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Récupération - incertitude'))

fig.update_layout(
    title='Performance evolution with redis',
    xaxis_title='nb_data',
    yaxis_title='Values',
)


pio.write_html(fig, file='graphe_redis.html', auto_open=True)

# Uncertainty curves for each operation in SQLite
sqlite_insert_uncertainty_curve_pos, sqlite_insert_uncertainty_curve_neg = compute_uncertainty_curve(mean_sqlite_insert,std_sqlite_insert)
sqlite_retrieval_uncertainty_curve_pos, sqlite_retrieval_uncertainty_curve_neg = compute_uncertainty_curve(mean_sqlite_retrieval,std_sqlite_retrieval)
sqlite_update_uncertainty_curve_pos, sqlite_update_uncertainty_curve_neg = compute_uncertainty_curve(mean_sqlite_update,std_sqlite_update)
sqlite_delete_uncertainty_curve_pos, sqlite_delete_uncertainty_curve_neg = compute_uncertainty_curve(mean_sqlite_delete,std_sqlite_delete)

data_sqlite = {
    'nb_data': list_nb_data,
    'Insertion': mean_sqlite_insert, 
    'Selection': mean_sqlite_retrieval, 
    'Mise a jour': mean_sqlite_update, 
    'Suppression': mean_sqlite_delete,
    'Insertion_std_borne_sup': sqlite_insert_uncertainty_curve_pos,
    'Selection_std_borne_sup': sqlite_retrieval_uncertainty_curve_pos,
    'Mise a jour_std_borne_sup': sqlite_update_uncertainty_curve_pos,
    'Suppression_std_borne_sup': sqlite_delete_uncertainty_curve_pos,
    'Insertion_std_borne_inf': sqlite_insert_uncertainty_curve_neg,
    'Selection_std_borne_inf': sqlite_retrieval_uncertainty_curve_neg,
    'Mise a jour_std_borne_inf': sqlite_update_uncertainty_curve_neg,
    'Suppression_std_borne_inf': sqlite_delete_uncertainty_curve_neg
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
                         line=dict(color='rgba(255,255,255,0)'), name='Insertion - incertitude'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Suppression_std_borne_sup'].tolist() + df_sqlite['Suppression_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(255,0,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Suppression - incertitude'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Mise a jour_std_borne_sup'].tolist() + df_sqlite['Mise a jour_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(0,255,0,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Mise a jour - incertitude'))

fig.add_trace(go.Scatter(x=df_sqlite['nb_data'].tolist() + df_sqlite['nb_data'].tolist()[::-1],
                         y=df_sqlite['Selection_std_borne_sup'].tolist() + df_sqlite['Selection_std_borne_inf'].tolist()[::-1],
                         fill='toself', fillcolor='rgba(128,0,128,0.2)',
                         line=dict(color='rgba(255,255,255,0)'), name='Récupération - incertitude'))

fig.update_layout(
    title='Performance evolution with SQLite',
    xaxis_title='nb_data',
    yaxis_title='Values',
)

pio.write_html(fig, file='graphe_sqlite.html', auto_open=True)

