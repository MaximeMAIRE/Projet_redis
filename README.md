----------------------------------------------------------
# Install

```
info@pcj413:~/Documents/redis$ python3 test.py
info@pcj413:~/Documents/redis$ chmod +x retrieve_data.sh
info@pcj413:~/Documents/redis$ ./retrieve_data.sh
Value for key1: value1
Value for key2: value2
```

---------------------------------------------------------
# Documentation

To get all keys from the database (2 possibilities) (three is no possibility to get all keys AND values):

SCAN allows to get all values from a database

If you do SCAN 0 (after executing test.py and addd_measure.py), you get

1) "0"
2) 1) "key2"
   2) "key1"
   3) "my_key"

where 1) "0"
is the next cursor position (you can do SCAN next_cursor_position to get the rest of the data)
and 2) is a set of all keys


The other possilbility is:
KEYS *

DEL key to delete a key.

MULTI permet d'exécuter plusieurs commandes à la fois (EXEC pour exécuter toutes les lignes après MULTI ; les instructions doivent être écrites une par une):

MULTI
TIME
SET key10 value 10
TIME
EXEC

-------------------------------------------------------
# Justification

Nous choisissons bash pour effecuter les calculs de temps. En effet, redis-cli ne permet pas d'exécuter plusieurs infor
Nous choisissons Python pour insérer des données ne nécessitant de calculs de temps, car Python ralentit les temps de calcul en passant par une bibliothèque. ## RENDRE CA PLUS COMPREHENSIBLE.

Nous supposons que le temps d'eécution et d'insertion varie selon la taille de la base de données (si celle-ci est déjà un peu remplie, nous supposons que le temps d'exécution et d'insertion est plus long). De ce fait, nous allons tester sur des bases de données vides et un peu plus remplies.

Jeu de données: https://www.kaggle.com/datasets/anth7310/mental-health-in-the-tech-industry/
Expliquer les différentes colonnes et tables

On a récupéré toutes les données de sqlite en faisant un join sur les tables pour les insérer dans redis et parvenir au même "état" dans redis que dans sqlite. Le test sur l'insertion et la déletion se fait dans cet état.

On a pas print nos résultats dans la récupération de données parce que le pint prend un peu de temps d'exécution et l'on souhaitait être authentique quant au temps de récupération.

Nous avons remarqués qu'en retirant les cursors pour l'insertion, nous n'obtenons en moyenne une performance équivalente. En revanche, pour la récupération de données, nous obtenons des performances accrues en incluant le cursor (de l'ordre d'environ 12%: passage de 9*10**-5 secondes à 1*10**-4 secondes (après une dizaine d'essais)).

Nous avons mis deux insert pour la base de données sqlite parce qu'on touche deux tables lors d el'ajout contrairement à redis.

Passer en MySQL:
- Installer MySQL
- CREATE DATABASE mental_health
- sqlite3 mental_health.sqlite .dump > mental_health.sql

---------------------------------------------------------

# Objectifs

Chaque case est remplie avec le résultat pour une moyenne sur 1000 requêtes.

xxxxxxx |                 redis              |                sqlite             |
----------------------------------------------------------------------------------
xxxxxxxx | Insert | Select | Update | Delete | Insert | Select | Update | Delete |
----------------------------------------------------------------------------------
Python   |        |        |        |        |        |        |        |        |
----------------------------------------------------------------------------------


--------------------------------------------------------

# Résultats

Temps de connexion à la base de données redis:
0.00010085105895996094 secondes.
Temps de connexion à la base de données sqlite:
7.009506225585938e-05 secondes.
Temps d'insertion de cent données sur redis:
3.822803497314453e-05 secondes.

Temps de récupération de cent données sur redis:
3.7994384765625e-05 secondes.

Temps d'insertion de cent données sur sqlite:
0.009390020370483398 secondes.

Temps de récupération de cent données sur sqlite:
0.00010430574417114258 secondes.

La connexion à Redis est plus lente que pour la base de données sqlite, en revanche, les traitements sont plus rapides sur la base de données Redis.


--------------------------------------------------------
# TÂCHES RESTANTES

- Convertir en .ipynb
- Indexation
- Fragmentation & réplication
- Rapport
- Samuel a fait un graphique qui vérifie le temps pris pour 100, 1000, 10000 insertions, .... Ca évolue quasi linéairement. 
