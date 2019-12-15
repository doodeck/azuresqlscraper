# oratest.py

from __future__ import print_function
from hdbcli import dbapi
import credentials


cnxn = dbapi.connect(credentials.server, credentials.port, credentials.username, credentials.password)
print (cnxn.isconnected())

c = cnxn.cursor()
c.execute("SELECT * FROM DUMMY")
for row in c:
    print(row)
c.close()
del c
cnxn.close()
del cnxn

'''
cursor = connection.cursor()
cursor.execute("""
    SELECT first_name, last_name
    FROM employees
    WHERE department_id = :did AND employee_id > :eid""",
    did = 50,
    eid = 190)
for fname, lname in cursor:
    print("Values:", fname, lname)
'''
