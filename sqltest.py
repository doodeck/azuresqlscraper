import pyodbc
import credentials

driver= '{ODBC Driver 17 for SQL Server}'
connectstr = 'DRIVER='+driver+';SERVER='+credentials.server+';PORT=1433;DATABASE='+credentials.database+';UID='+credentials.username+';PWD='+ credentials.password
cnxn = pyodbc.connect(connectstr)
cursor = cnxn.cursor()
# cursor.execute("SELECT TOP 20 pc.Name as CategoryName, p.name as ProductName FROM [SalesLT].[ProductCategory] pc JOIN [SalesLT].[Product] p ON pc.productcategoryid = p.productcategoryid")
cursor.execute("SELECT * FROM SalesLT.Product")
row = cursor.fetchone()
while row:
    print (str(row[0]) + " " + str(row[1]))
    row = cursor.fetchone()
