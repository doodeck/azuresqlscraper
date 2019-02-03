import json
import urllib.request as urllib
import pyodbc
import credentials

url = "http://worldclockapi.com/api/json/cet/now" # "http://httpbin.org/get"
response = urllib.urlopen(url)
data = response.read()
values = json.loads(data)
print (values)
print (type(values["currentDateTime"]), type(values["currentFileTime"]))

driver = '{ODBC Driver 17 for SQL Server}'
connectstr = 'DRIVER='+driver+';SERVER='+credentials.server+';PORT=1433;DATABASE='+credentials.database+';UID='+credentials.username+';PWD='+ credentials.password
cnxn = pyodbc.connect(connectstr)
cursor = cnxn.cursor()
insert_sql = "INSERT INTO Scraper.Timestamps VALUES ('" + values["currentDateTime"] + "', " + str(values["currentFileTime"]) + ")"
print(insert_sql)
cursor.execute(insert_sql)
cnxn.commit()
