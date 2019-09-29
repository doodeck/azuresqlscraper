# oratest.py

from __future__ import print_function
import cx_Oracle
import credentials

'''
Connect with Oracle Test User HR
For Cloud Autonomous DB the HR schema must be installed first:
From Web SQL Developer as DBA:
CREATE USER hr IDENTIFIED BY ***************;
GRANT CREATE SESSION TO HR;
GRANT dwrole TO HR;
GRANT UNLIMITED TABLESPACE TO HR;
Download sample schema: https://github.com/oracle/db-sample-schemas
From command line:
sqlplus HR/**************@db**********_low < hr_drop.sql
sqlplus HR/**************@db**********_low < hr_cre.sql
...
sqlplus HR/**************@db**********_low < hr_analz.sql
OR as DBA:
sqlplus HR/**************@db**********_low < hr_main.sql
'''

connection = cx_Oracle.connect(credentials.username, credentials.password, credentials.database)

cursor = connection.cursor()
cursor.execute("""
    SELECT first_name, last_name
    FROM employees
    WHERE department_id = :did AND employee_id > :eid""",
    did = 50,
    eid = 190)
for fname, lname in cursor:
    print("Values:", fname, lname)
