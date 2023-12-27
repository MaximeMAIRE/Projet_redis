import sqlite3

con = sqlite3.connect("mental_health_200000.sqlite")
cur = con.cursor()
ans = cur.execute("SELECT count(*) from question")
for row in ans:
    print(row)
ans = cur.execute("SELECT count(*) from answer")
for row in ans:
    print(row)