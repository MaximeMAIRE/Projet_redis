import sqlite3

con = sqlite3.connect("mental_health.sqlite")
cur = con.cursor()
ans = cur.execute("SELECT count(*) from question")
for row in ans:
    print(row)
ans = cur.execute("SELECT count(*) from answer")
for row in ans:
    print(row)