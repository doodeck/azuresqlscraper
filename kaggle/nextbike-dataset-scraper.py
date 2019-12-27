#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# jupyter nbconvert --to script nextbike-dataset-scraper.ipynb

import json
import psutil
import requests

# TODO: protect against double execution (lock file)

namespace = "NextBscraper"
# TODO Use Pluggable / Container Database wehrerever possible instead

# interesting how much memory do we need for this kind of processing
print("before: ", psutil.virtual_memory())
r = requests.get('https://nextbike.net/maps/nextbike-live.json') # ?place=26816 ?city=176 ?place=26816 ?city=176

nextbike_dict = r.json()
"""
with open('nextbike-live_4.json') as f:
    nextbike_dict = json.load(f)
"""
print("after: ", psutil.virtual_memory())


# ## Grope through the data structure

# In[ ]:


print(nextbike_dict.keys())
countrynodescnt = len(nextbike_dict['countries'])
print(countrynodescnt)
if countrynodescnt < 1:
    print ('Suspiciously few nodes found, aborting: ', countrynodescnt)
    quit()

# print(nextbike_dict['countries'][0])
# print(nextbike_dict['countries'][0]["cities"])
# print(nextbike_dict['countries'][0]["cities"][0]['places'])
# print(nextbike_dict['countries'][0]['cities'][0]['places'][0]['bike_list'])


# ## Decide if we are going to use Sqlite, Azure or Oracle

# In[ ]:


from enum import Enum
class DBType(Enum):
    SQLITE = 1
    AZURE = 2 # Azure SQL
    ORACLE = 3 # Oracle Cloud Autonomous DB
    HXE = 4 # SAP Hana Express Edition

dbType = DBType.HXE #  DBType.ORACLE # DBType.SQLITE

import os.path

credentialsfilename = "../credentials.py" # similar format for Azure, Oracle and Hana, not used for Sqlite
# useazuresql = os.path.isfile(credentialsfilename)

if dbType == DBType.AZURE:
    # Warning: Azure free tier for 12 months is only 250GB ;-)
    import pyodbc
    if not os.path.isfile(credentialsfilename):
        print('Missing credentials file!')
    print ('Using Azure SQL, schema: ', namespace)
elif dbType == DBType.ORACLE:
    import cx_Oracle
    if (not os.path.isfile(credentialsfilename)):
        print('Missing credentials file!')
    print ('Using Oracle SQL') # , schema: ', namespace)
elif dbType == DBType.HXE:
    from hdbcli import dbapi
    if (not os.path.isfile(credentialsfilename)):
        print('Missing credentials file!')
    print ('Using Hana XE SQL, schema: ', namespace)
else:
    import sqlite3
    print ('Using Sqlite')

def get_azure_connect_str():
    from runpy import run_path
    credentials = run_path(credentialsfilename)
    # prepare ODBC string
    driver = '{ODBC Driver 17 for SQL Server}'
    return 'DRIVER='+driver+';SERVER='+credentials['server']+';PORT=1433;DATABASE='+credentials['database']+';UID='+credentials['username']+';PWD='+ credentials['password']

def get_oracle_connection():
    from runpy import run_path
    credentials = run_path(credentialsfilename)
    # print (credentials.keys())
    return cx_Oracle.connect(credentials['username'], credentials['password'], credentials['database'])

def get_hxe_connection():
    from runpy import run_path
    credentials = run_path(credentialsfilename)
    # print (credentials.keys())
    return dbapi.connect(credentials['server'], credentials['port'], credentials['username'], credentials['password'])

# TODO: apply to all fields where the problem theoretically may appear
# Double quotes are disturbing Kaggle parsing and visualization of CSV files
# commas are dusturbing only at CSV export level (for obvious reasons)
def replace_illegals(str):
    return str.replace(",", ";").replace('"', "'")

def isnotebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter


# ## Create Sqlite DB Tables

# In[ ]:


sqlitefile = "./DB/database.sqlite"

def create_sqlite_tables():
    conn = sqlite3.connect(sqlitefile) # , timeout=10)

    c = conn.cursor()

    # TODO Remove before flight
    '''
    c.execute('DROP TABLE IF EXISTS countries')
    c.execute('DROP TABLE IF EXISTS cities')
    c.execute('DROP TABLE IF EXISTS places')
    c.execute('DROP TABLE IF EXISTS bike_list')
    '''

    # coutries table
    c.execute('''CREATE TABLE IF NOT EXISTS countries
                 (
                   guid INTEGER PRIMARY KEY AUTOINCREMENT,
                   country VARCHAR(2),
                   country_name TEXT,
                   created INTEGER
                   -- updated INTEGER
                 )''')

    # cities table
    c.execute('''CREATE TABLE IF NOT EXISTS cities
                 (
                   guid INTEGER PRIMARY KEY AUTOINCREMENT,
                   uid INTEGER,
                   name TEXT,
                   countryguid INTEGER,
                   created INTEGER,
                   -- updated INTEGER,
                   FOREIGN KEY(countryguid) REFERENCES countries(guid)
                 )''')

    # places table
    c.execute('''CREATE TABLE IF NOT EXISTS places
                 (
                   guid INTEGER PRIMARY KEY AUTOINCREMENT,
                   uid INTEGER,
                   name TEXT,
                   cityguid INTEGER,
                   created INTEGER,
                   FOREIGN KEY(cityguid) REFERENCES cities(guid)
                 )''')

    # bike_list table
    c.execute('''CREATE TABLE IF NOT EXISTS bike_list
                 (
                   guid INTEGER PRIMARY KEY AUTOINCREMENT,
                   number INTEGER,
                   placeguid INTEGER,
                   appeared INTEGER, -- timestamp the bike appeared on this station
                   disappeared INTEGER, -- timestamp the bike disappeared from this station
                   FOREIGN KEY(placeguid) REFERENCES places(guid)
                 )''')

    c.execute('DROP TABLE IF EXISTS tmp_bike_list')
    c.execute('''CREATE TABLE IF NOT EXISTS tmp_bike_list -- current bike list, needed to figure our bikes which disappeared
                 (
                   number INTEGER PRIMARY KEY, -- assumption: the bikes have globally unique numbers - TODO: to be tested
                   placeguid INTEGER,
                   FOREIGN KEY(placeguid) REFERENCES places(guid)
                 )''')

    # Save (commit) the changes
    conn.commit()

    c.close()
    del c

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()
    del conn
    return

if dbType == DBType.SQLITE:
    create_sqlite_tables()


# ## Create Azure SQL Tables (if applicable)

# In[ ]:


def create_azure_tables():
    connectstr = get_azure_connect_str()
    cnxn = pyodbc.connect(connectstr)
    c = cnxn.cursor()
    '''
    c.execute("SELECT TOP 10 * FROM SalesLT.Product")
    row = c.fetchone()
    while row:
        print (str(row[0]) + " " + str(row[1]))
        row = c.fetchone()
    '''
    

    # countries table
    c.execute("""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                WHERE TABLE_SCHEMA = '""" + namespace + """' AND  TABLE_NAME = 'countries')
                  CREATE TABLE """ + namespace + '''.countries (
                    guid INT PRIMARY KEY IDENTITY (1,1),
                    country VARCHAR(2) NOT NULL,  -- I don't expect Unicode here, ever
                    country_name NVARCHAR(40), -- SELECT MAX(LENGTH(country_name)) FROM countries: 22
                    lat FLOAT(53),
                    lng FLOAT(53),
                    created datetime NOT NULL,
                    -- updated datetime NOT NULL
                    -- TODO: country list will also change in the future: need disappeared or analog
                    INDEX countries_index (country, country_name)
             )''')

    # cities table
    c.execute("""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                WHERE TABLE_SCHEMA = '""" + namespace + """' AND  TABLE_NAME = 'cities')
                CREATE TABLE """ + namespace + '''.cities (
                guid int PRIMARY KEY IDENTITY (1,1),
                uid INTEGER NOT NULL,
                name NVARCHAR(50) NOT NULL, -- SELECT MAX(LENGTH(name)) FROM cities: 36
                countryguid INT NOT NULL FOREIGN KEY REFERENCES ''' + namespace + '''.countries(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                lat FLOAT(53),
                lng FLOAT(53),
                created datetime NOT NULL,
                -- TODO: cities list will also change in the future: need disappeared or analog
                INDEX cities_index (uid, name, countryguid)
             )''')

    # places table
    c.execute("""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                WHERE TABLE_SCHEMA = '""" + namespace + """' AND  TABLE_NAME = 'places')
                CREATE TABLE """ + namespace + '''.places (
                guid int PRIMARY KEY IDENTITY (1,1),
                uid INTEGER NOT NULL,
                name NVARCHAR(100) NOT NULL, -- SELECT MAX(LENGTH(name)) FROM places: 81
                bike BIT NOT NULL,
                spot BIT NOT NULL, -- spot should always be !bike, but is it?
                cityguid INT NOT NULL FOREIGN KEY REFERENCES ''' + namespace + '''.cities(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                lat FLOAT(53),
                lng FLOAT(53),
                created datetime NOT NULL,
                disappeared datetime,
                INDEX places_index (uid, name, bike, spot, cityguid, lat, lng, created, disappeared),
                INDEX places_disapp (disappeared) INCLUDE ([uid])
                -- Recommendation Azure: CREATE NONCLUSTERED INDEX [nci_wi_places_8D60EAA10CDAC3940967E78902A4171B] ON [NextBscraper].[places] ([disappeared]) INCLUDE ([uid]) WITH (ONLINE = ON)
             )''')
    # CREATE INDEX i1 ON t1 (col1); 

    # bike_list table
    c.execute("""IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                WHERE TABLE_SCHEMA = '""" + namespace + """' AND  TABLE_NAME = 'bike_list')
                CREATE TABLE """ + namespace + '''.bike_list (
                guid int PRIMARY KEY IDENTITY (1,1),
                number INT,
                placeguid INT NOT NULL FOREIGN KEY REFERENCES ''' + namespace + '''.places(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                appeared datetime NOT NULL,
                disappeared datetime,
                INDEX blist_index_all (number, placeguid, appeared, disappeared),
                INDEX blist_index_plguid (placeguid),
                INDEX blist_disapp (disappeared),
                INDEX blist_disnupl([disappeared], [number], [placeguid])
             )''')

    '''
    Recommendation from Azure, last updated 04.03.2019:
    CREATE NONCLUSTERED INDEX [nci_wi_bike_list_D436CEC18B88499A1CC92C5B28F78508] ON [NextBscraper].[bike_list] ([placeguid]) WITH (ONLINE = ON)
    '''
    '''
    Recommendation from Azure, last updated: 11.3.2019
    '''

    cnxn.commit()
    c.close() # TODO: use with ... to clean up resources in all cases
    del c
    cnxn.close()     #<--- Close the connection
    del cnxn
    return

if dbType == DBType.AZURE:
    create_azure_tables()


# ## Create Oracle Autonomous DB Table (if applicable)

# In[ ]:


def create_oracle_tables():
    # TODO: hier not implemented yet
    cnxn = get_oracle_connection() # connection
    c = cnxn.cursor() # cursor
    c.execute("""
      SELECT SYSDATE FROM DUAL
    """)
    cnxn.commit()
    c.close()
    del c

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    cnxn.close()
    del cnxn
    return

if dbType == DBType.ORACLE:
    create_oracle_tables()


# ## Create Hana Express Edition DB Table (if applicable)

# In[ ]:


def create_hxe_tables():
    cnxn = get_hxe_connection() # connection
    c = cnxn.cursor()
    
    # TODO Remove before flight
    c.execute('DROP SCHEMA ' + namespace + ' CASCADE')
    c.execute('CREATE SCHEMA ' + namespace)

    '''
    c.execute('DROP TABLE ' + namespace + '._tmp_places')
    c.execute('DROP TABLE ' + namespace + '._tmp_bike_list')
    c.execute('DROP TABLE ' + namespace + '.bike_list')
    c.execute('DROP TABLE ' + namespace + '.places')
    c.execute('DROP TABLE ' + namespace + '.cities')
    c.execute('DROP TABLE ' + namespace + '.countries')
    '''
    
    # countries table
    c.execute("""CREATE COLUMN TABLE """ + namespace + '''.countries (
                    guid INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, -- PRIMARY KEY IDENTITY (1,1),
                    country VARCHAR(2) NOT NULL,  -- I don't expect Unicode here, ever
                    country_name NVARCHAR(40), -- SELECT MAX(LENGTH(country_name)) FROM countries: 22
                    lat FLOAT(53),
                    lng FLOAT(53),
                    created SECONDDATE NOT NULL
                    -- updated datetime NOT NULL
                    -- TODO: country list will also change in the future: need disappeared or analog
                    -- INDEX countries_index (country, country_name)
                    )''')
    c.execute("CREATE INDEX  " + namespace + "._idx_countries ON " + namespace + ".countries(country, country_name)")

    # cities table
    c.execute("""CREATE COLUMN TABLE """ + namespace + '''.cities (
                guid INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
                uid INTEGER NOT NULL,
                name NVARCHAR(50) NOT NULL, -- SELECT MAX(LENGTH(name)) FROM cities: 36
                countryguid INTEGER,
                lat FLOAT(53),
                lng FLOAT(53),
                created SECONDDATE NOT NULL,
                FOREIGN KEY (countryguid) REFERENCES  ''' + namespace + '''.countries(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                -- TODO: cities list will also change in the future: need disappeared or analog
                -- INDEX cities_index (uid, name, countryguid)
             )''')
    c.execute("CREATE INDEX  " + namespace + "._idx_cities ON " + namespace + ".cities(uid, name, countryguid)")

    # places table
    c.execute("""CREATE COLUMN TABLE """ + namespace + '''.places (
                guid INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
                uid INTEGER NOT NULL,
                name NVARCHAR(100) NOT NULL, -- SELECT MAX(LENGTH(name)) FROM places: 81
                bike BOOLEAN NOT NULL,
                spot BOOLEAN NOT NULL, -- spot should always be !bike, but is it?
                cityguid INTEGER NOT NULL,
                lat FLOAT(53),
                lng FLOAT(53),
                created SECONDDATE NOT NULL,
                disappeared SECONDDATE,
                FOREIGN KEY (cityguid) REFERENCES ''' + namespace + '''.cities(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                -- INDEX places_index (uid, name, bike, spot, cityguid, lat, lng, created, disappeared),
                -- INDEX places_disapp (disappeared) INCLUDE ([uid])
                -- Recommendation Azure: CREATE NONCLUSTERED INDEX [nci_wi_places_8D60EAA10CDAC3940967E78902A4171B] ON [NextBscraper].[places] ([disappeared]) INCLUDE ([uid]) WITH (ONLINE = ON)
             )''')
    c.execute("CREATE INDEX  " + namespace + "._idx_places ON " + namespace + ".places(uid, name, bike, spot, cityguid, lat, lng, created, disappeared)")
    c.execute("CREATE INDEX  " + namespace + "._idx_places_disapp ON " + namespace + ".places(uid, disappeared)")

    # bike_list table
    c.execute("""CREATE COLUMN TABLE """ + namespace + '''.bike_list (
                guid INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
                number INTEGER,
                placeguid INTEGER NOT NULL,
                appeared SECONDDATE NOT NULL,
                disappeared SECONDDATE,
                FOREIGN KEY (placeguid) REFERENCES ''' + namespace + '''.places(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                -- INDEX blist_index_all (number, placeguid, appeared, disappeared),
                -- INDEX blist_index_plguid (placeguid),
                -- INDEX blist_disapp (disappeared),
                -- INDEX blist_disnupl([disappeared], [number], [placeguid])
             )''')
    c.execute("CREATE INDEX  " + namespace + "._idx_blist_all ON " + namespace + ".bike_list(number, placeguid, appeared, disappeared)")
    c.execute("CREATE INDEX  " + namespace + "._idx_blist_plguid ON " + namespace + ".bike_list(placeguid)")
    c.execute("CREATE INDEX  " + namespace + "._idx_blist_disapp ON " + namespace + ".bike_list(disappeared)")
    c.execute("CREATE INDEX  " + namespace + "._idx_blist_disnupl ON " + namespace + ".bike_list(number, placeguid, disappeared)")

    c.execute('CREATE COLUMN TABLE ' + namespace + '''._tmp_bike_list (
                number INTEGER PRIMARY KEY, -- assumption: the bikes have globally unique numbers - TODO: to be tested
                placeguid INTEGER NOT NULL,
                FOREIGN KEY (placeguid) REFERENCES ''' + namespace + '''.places(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                -- INDEX tmp_blist_index (placeguid)
             )''')
    c.execute("CREATE INDEX  " + namespace + "._idx_tmp_bike_list ON " + namespace + "._tmp_bike_list(placeguid)")

    c.execute('CREATE COLUMN TABLE ' + namespace + '''._tmp_places (
                uid INTEGER PRIMARY KEY, -- assumption: the places have globally unique numbers - TODO: to be tested
                cityguid INTEGER NOT NULL,
                FOREIGN KEY (cityguid) REFERENCES ''' + namespace + '''.cities(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                -- INDEX tmp_places_index (cityguid)
             )''')
    c.execute("CREATE INDEX  " + namespace + "._idx_tmp_places ON " + namespace + "._tmp_places(cityguid)")

    '''
    c.execute("SELECT * FROM DUMMY")
    for row in c:
        print(row)
    '''
    c.close()
    del c
    cnxn.close()
    del cnxn
    return

if dbType == DBType.HXE and isnotebook():
    create_hxe_tables()


# ## Transfer Data from JSON to SAP HANA DB (if applicable)

# In[ ]:


def update_hxe_tables():
    from timeit import default_timer # as timer
    cnxn = get_hxe_connection() # connection
    c = cnxn.cursor()

    # t = time.process_time()
    start = default_timer()

    c.execute('DELETE FROM ' + namespace + '._tmp_bike_list') # TODO: check if exists
    c.execute('DELETE FROM ' + namespace + '._tmp_places') # TODO: check if exists

    for country in nextbike_dict['countries']:
        # print ('JSON country: ', country["country"], country["country_name"])
        c.execute('SELECT guid FROM ' + namespace + '.countries WHERE country = ? AND country_name = ?',
                                    (country["country"], country["country_name"])) # .fetchall()

        rows = c.fetchall()
        if len(rows) > 1:
            print ('Too many entries for ', country["country"], country["country_name"], ' : ', len(rows))
        elif len(rows) == 1:
            currcountryguid = rows[0][0]
            # print ('already exists currcountryguid(1): ', currcountryguid)
        else:
            # print ('about to insert: ', country["country"], country["country_name"])
            if True != c.execute('INSERT INTO ' + namespace + '.countries (country, country_name, lat, lng, created) VALUES (?,?,?,?,NOW())',
                  (country["country"], country["country_name"], country["lat"], country["lng"])): #TODO: React on lat/lng changes
                print ('INSERT failed: ', country["country"], country["country_name"])
            else:
                """
hdbsql SYSTEMDB=> select COLUMN_NAME,COLUMN_ID from table_columns where table_name = 'COUNTRIES' AND schema_name='NEXTBSCRAPER';
COLUMN_NAME,COLUMN_ID
"COUNTRY",176994
"COUNTRY_NAME",176995
"CREATED",176998
"GUID",176993
"LAT",176996
"LNG",176997
6 rows selected (overall time 1075.699 msec; server time 452 usec)

hdbsql SYSTEMDB=> select * from sequences where sequence_name like '%176993%';
SCHEMA_NAME,SEQUENCE_NAME,SEQUENCE_OID,START_NUMBER,MIN_VALUE,MAX_VALUE,INCREMENT_BY,IS_CYCLED,RESET_BY_QUERY,CACHE_SIZE,CREATE_TIME
"NEXTBSCRAPER","_SYS_SEQUENCE_176993_#0_#",177003,1,1,9223372036854775807,1,"FALSE","select ifnull(MAP( mod((max( to_decimal(\"GUID\")) - (1) ),1),  0, max(\"GUID\") +1,  max(\"GUID\") - mod((max( \"GUID\") - (1)), 1 ) +1 ) , 1) from \"NEXTBSCRAPER\".\"COUNTRIES\"",1,"2019-12-24 08:58:21.727000000"
1 row selected (overall time 234.386 msec; server time 474 usec)

hdbsql SYSTEMDB=> SELECT NEXTBSCRAPER._SYS_SEQUENCE_176993_#0_#.CURRVAL FROM DUMMY;                                             
* 326: CURRVAL of given sequence is not yet defined in this session: cannot find currval location by session_id:102104, seq id:177003, seq version:1:  (at pos 7)  SQLSTATE: HY000
                """
                c.execute('SELECT MAX(guid) FROM ' + namespace + '.countries') # TODO: sequence will wrap up at some point
                currcountryguid = c.fetchone()[0]
                # print ('currcountryguid(2): ', currcountryguid)

        for city in country['cities']:
            # print ('JSON city: ', city["uid"], city["name"])
            c.execute('SELECT guid FROM ' + namespace + '.cities WHERE uid = ? AND name = ? AND countryguid = ?',
                                     (city["uid"], city["name"], currcountryguid)) #.fetchall() # .encode('utf8')
            rows = c.fetchall()
            if len(rows) > 1:
                print ('Too many entries for ', city["uid"], city["name"], currcountryguid, ' : ', len(rows)) # .encode('utf8')
            elif len(rows) == 1:
                currcityguid = rows[0][0]
                # print ('already exists currcityguid(1): ', currcityguid)
            else:
                # The reason it's sometimes failing are Unicode characters:
                # [{"uid":128,"lat":56.9453,"lng":24.1033,"zoom":12,"maps_icon":"","alias":"riga","break":false,"name":"R\u012bga"
                # https://stackoverflow.com/questions/16565028/pyodbc-remove-unicode-strings
                if True != c.execute('INSERT INTO ' + namespace + '.cities (uid, name, countryguid, lat, lng, created) VALUES (?,?,?,?,?,NOW())',
                      (city["uid"], city["name"], currcountryguid, city["lat"], city["lng"])): #TODO: React on lat/lng changes
                    print ('INSERT failed: ', city["uid"], city["name"], currcountryguid) # .encode('utf8')
                else:
                    c.execute('SELECT MAX(guid) FROM ' + namespace + '.cities') # TODO: sequence will wrap up at some point
                    currcityguid = c.fetchone()[0]
                    # print ('currcityguid(2): ', currcityguid)

            for place in city['places']:
                spot = place['spot'] # None if place['spot'] is None else 1 if place['spot'] == True else 0
                bike = place['bike'] # None if place['bike'] is None else 1 if place['bike'] == True else 0
                # print ('lat: ', place['lat'], 'lng: ', place['lng'])
                # print ('name1: ', place['name'])
                name = replace_illegals(place['name'])
                # print ('name2: ', name)
                # print ("place['spot']: ", place['spot'], type(place['spot']), spot)
                # print ("place['bike']: ", place['bike'], type(place['bike']), bike)
                # print ('JSON place: ', place['uid'], name)
                # ignore names for non spots, they apparently change
                c.execute('SELECT guid FROM ' + namespace + '.places WHERE uid = ? AND (name = ? OR spot = FALSE) AND cityguid = ? AND lat = ? AND lng = ? AND disappeared IS NULL',
                                         (place["uid"], name, currcityguid, place["lat"], place["lng"],))
                rows = c.fetchall()
                if len(rows) > 1:
                    print ('Too many entries for ', place["uid"], name, currcityguid, ' : ', len(rows))
                elif len(rows) == 1:
                    currplaceguid = rows[0][0]
                    # print ('currplaceguid(1): ', currplaceguid)
                else:
                    c.execute('UPDATE ' + namespace + '''.places SET disappeared = NOW()
                              WHERE disappeared IS NULL AND uid = ? AND (name = ? OR spot = FALSE) AND cityguid = ? AND (lat != ? OR lng != ?)''',
                              (place["uid"], name, currcityguid, place["lat"], place["lng"]))
                    if True != c.execute('INSERT INTO ' + namespace + '.places (uid, name, bike, spot, cityguid, lat, lng, created) VALUES (?,?,?,?,?,?,?,NOW ())',
                          (place["uid"], name, bike, spot, currcityguid, place["lat"], place["lng"])): # TODO: verify duplicate using bike & spot as well
                        print ('INSERT failed: ', place["uid"], name, bike, spot, currcityguid)
                    else:
                        c.execute('SELECT MAX(guid) FROM ' + namespace + '.places') # TODO: sequence will wrap up at some point
                        currplaceguid = c.fetchone()[0]

                        # print ('currplaceguid(2): ', currplaceguid)
                c.execute('UPSERT ' + namespace + '._tmp_places (uid, cityguid) VALUES (?,?) WHERE uid=? AND cityguid=?', # duplicates should never happen
                          (place["uid"], currcityguid, place["uid"], currcityguid,)) # TODO: should I care about spot/bike here?

                for bike in place['bike_list']:
                    # print ('JSON bike: ', bike['number'])
                    # bikeuid = int(str(place['uid']) + str(bike['number'])) # Integers in Python 3 are of unlimited size.
                    c.execute('SELECT guid FROM ' + namespace + '.bike_list WHERE number = ? AND placeguid = ? AND disappeared IS NULL',
                                             (bike['number'], currplaceguid))
                    rows = c.fetchall()
                    if len(rows) > 1:
                        print ('Too many entries for ', bike['number'], currplaceguid, ' : ', len(rows))
                    elif len(rows) == 1:
                        currentnumber = rows[0][0] # value not used other than for testing
                        # print ('currentnumber: ', currentnumber)
                    else:
                        # disappear the bike from the old location, unless already gone:
                        c.execute('UPDATE ' + namespace + '''.bike_list SET disappeared = NOW()
                                  WHERE disappeared IS NULL AND number = ? AND placeguid != ?''', (bike['number'], currplaceguid))
                        # Insert the bike into the new location:
                        c.execute('INSERT INTO ' + namespace + '.bike_list (number, placeguid, appeared, disappeared) VALUES (?,?,NOW(), NULL)',
                                  (bike['number'], currplaceguid))
                    c.execute('UPSERT ' + namespace + '._tmp_bike_list (number, placeguid) VALUES (?,?) WHERE number=? AND placeguid=?',
                              (bike['number'], currplaceguid, bike['number'], currplaceguid,))

    # Places may disappear - in particular when they are bikes which can be left anywhere
    c.execute('UPDATE ' + namespace + '''.places SET disappeared = NOW() WHERE disappeared IS NULL AND uid NOT IN
                 (SELECT ''' + namespace + '._tmp_places.uid FROM ' + namespace + '._tmp_places JOIN ' + namespace + '''.places
                  ON (_tmp_places.cityguid = places.cityguid))''')

    # mark as dispperad bikes which do not apppear in the current JSON anymore - presumable under way
    c.execute('UPDATE ' + namespace + '''.bike_list SET disappeared = NOW() WHERE disappeared IS NULL AND number NOT IN
                 (SELECT ''' + namespace + '._tmp_bike_list.number FROM ' + namespace + '._tmp_bike_list JOIN ' + namespace + '''.bike_list
                  ON (_tmp_bike_list.placeguid = bike_list.placeguid))''')

    c.close()
    del c
    cnxn.close()
    del cnxn
    print ("Time elapsed: ", default_timer() - start)
    return

if dbType == DBType.HXE:
    update_hxe_tables()


# In[ ]:


"""
print (nextbike_dict['countries'][0]['cities'][0]['places'][0]['bike_list'])
del nextbike_dict['countries'][0]['cities'][0]['places'][0]['bike_list'][1]
print (nextbike_dict['countries'][0]['cities'][0]['places'][0]['bike_list'])
"""
print (isnotebook())


# ## Transfer Data from JSON to Azure DB (if applicable)

# In[ ]:


import time

def update_azure_tables():
    connectstr = get_azure_connect_str()
    cnxn = pyodbc.connect(connectstr)
    c = cnxn.cursor()

    c.execute('DROP TABLE IF EXISTS ' + namespace + '.#tmp_bike_list') # TODO: DELETE FROM instead of DROP TABLE
    c.execute('CREATE TABLE ' + namespace + '''.#tmp_bike_list (
                number INT PRIMARY KEY, -- assumption: the bikes have globally unique numbers - TODO: to be tested
                placeguid INT NOT NULL FOREIGN KEY REFERENCES ''' + namespace + '''.places(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                INDEX tmp_blist_index (placeguid)
             )''')

    c.execute('DROP TABLE IF EXISTS ' + namespace + '.#tmp_places') # TODO: DELETE FROM instead of DROP TABLE
    c.execute('CREATE TABLE ' + namespace + '''.#tmp_places (
                uid INT PRIMARY KEY, -- assumption: the places have globally unique numbers - TODO: to be tested
                cityguid INT NOT NULL FOREIGN KEY REFERENCES ''' + namespace + '''.cities(guid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                INDEX tmp_places_index (cityguid)
             )''')

    for country in nextbike_dict['countries']:
        # print ('JSON country: ', country["country"], country["country_name"])
        rows = c.execute('SELECT guid FROM ' + namespace + '.countries WHERE country = ? AND country_name = ?',
                                    (country["country"], country["country_name"])).fetchall()

        if len(rows) > 1:
            print ('Too many entries for ', country["country"], country["country_name"], ' : ', len(rows))
        elif len(rows) == 1:
            currcountryguid = rows[0][0]
            # print ('currcountryguid(1): ', currcountryguid)
        else:
            c.execute('INSERT INTO ' + namespace + '''.countries (country, country_name, lat, lng, created) SELECT ?,?,?,?,CAST ( GETDATE() as DATETIME )
                     WHERE NOT EXISTS (SELECT 1 FROM ''' + namespace + '.countries WHERE country = ? AND country_name = ?)',
                  (country["country"], country["country_name"], country["lat"], country["lng"], # int(time.time()),
                   country["country"], country["country_name"])) #TODO: React on lat/lng changes
            if c.rowcount != 1:
                print ('INSERT failed: ', country["country"], country["country_name"])
            else:
                currcountryguid = c.execute('SELECT @@IDENTITY').fetchone()[0]
                # print ('currcountryguid(2): ', currcountryguid)

        for city in country['cities']:
            # print ('JSON city: ', city["uid"], city["name"])
            rows = c.execute('SELECT guid FROM ' + namespace + '.cities WHERE uid = ? AND name = ? AND countryguid = ?',
                                     (city["uid"], city["name"], currcountryguid)).fetchall() # .encode('utf8')
            # SELECT CAST(fieldname AS VARCHAR) AS fieldname
            if len(rows) > 1:
                print ('Too many entries for ', city["uid"], city["name"], currcountryguid, ' : ', len(rows)) # .encode('utf8')
            elif len(rows) == 1:
                currcityguid = rows[0][0]
                # print ('currcityguid(1): ', currcityguid)
            else:
                # The reason it's sometimes failing are Unicode characters:
                # [{"uid":128,"lat":56.9453,"lng":24.1033,"zoom":12,"maps_icon":"","alias":"riga","break":false,"name":"R\u012bga"
                # https://stackoverflow.com/questions/16565028/pyodbc-remove-unicode-strings
                """
                print ('city INSERT imminent for: ', city["uid"], city["name"], type(city["name"]), currcountryguid) # .encode('utf8')
                row = c.execute('SELECT * FROM ' + namespace + ".cities WHERE uid = ? AND countryguid = ?",
                                         (city["uid"], currcountryguid)).fetchone() # .encode('utf8')
                while row:
                    print (str(row[0]) + "/" + str(row[1]) + "/" + str(row[2]) + "/" + str(row[3]) + "/" + str(row[4]))
                    row = c.fetchone()
                break
                """
                c.execute('INSERT INTO ' + namespace + '''.cities (uid, name, countryguid, lat, lng, created) SELECT ?,?,?,?,?,CAST ( GETDATE() as DATETIME )
                      WHERE NOT EXISTS (SELECT 1 FROM ''' + namespace + '.cities WHERE uid = ? AND name = ? AND countryguid = ?)',
                      (city["uid"], city["name"], currcountryguid, city["lat"], city["lng"], #TODO: React on lat/lng changes
                       city["uid"], city["name"], currcountryguid)) # .encode('utf8')
                if c.rowcount != 1:
                    print ('INSERT failed: ', city["uid"], city["name"], currcountryguid) # .encode('utf8')
                else:
                    currcityguid = c.execute('SELECT @@IDENTITY').fetchone()[0]
                    # print ('currcityguid(2): ', currcityguid)
            for place in city['places']:
                spot = None if place['spot'] is None else 1 if place['spot'] == True else 0
                bike = None if place['bike'] is None else 1 if place['bike'] == True else 0
                # print ('lat: ', place['lat'], 'lng: ', place['lng'])
                # print ('name1: ', place['name'])
                name = replace_illegals(place['name'])
                # print ('name2: ', name)
                # print ("place['spot']: ", place['spot'], type(place['spot']), spot)
                # print ("place['bike']: ", place['bike'], type(place['bike']), bike)
                '''
                WARNING, names of places sometimes do change
                SELECT uid, COUNT(uid) FROM NextBscraper.places GROUP BY uid HAVING COUNT(uid) > 1;
                SELECT * FROM NextBscraper.places WHERE uid = 48356;
                    GUID  UID   NAME       CITYGUID CREATED
                    10103 48356 BIKE       80       2019-02-24T23:20:18.5730000
                    14752 48356 BIKE 93679 80       2019-02-25T10:00:20.2630000
                    2136  48356 BIKE 93797 80       2019-02-24T21:42:08.5970000
                '''
                # print ('JSON place: ', place['uid'], name)
                # ignore names for non spots, they apparently change
                rows = c.execute('SELECT guid FROM ' + namespace + '.places WHERE uid = ? AND (name = ? OR spot = 0) AND cityguid = ? AND lat = ? AND lng = ? AND disappeared IS NULL',
                                         (place["uid"], name, currcityguid, place["lat"], place["lng"],)).fetchall()
                if len(rows) > 1:
                    print ('Too many entries for ', place["uid"], name, currcityguid, ' : ', len(rows))
                elif len(rows) == 1:
                    currplaceguid = rows[0][0]
                    # print ('currplaceguid(1): ', currplaceguid)
                else:
                    """
                    print ('Insert imminent for: ', place["uid"], name, currcityguid, len(rows))
                    row = c.execute('SELECT guid FROM ' + namespace + '.places WHERE uid = ? AND (name = ? OR spot = 0) AND cityguid = ?',
                                             (place["uid"], name, currcityguid)).fetchone()
                    while row:
                        print (str(row[0]) + " " + str(row[1]) + " " + str(row[2]))
                        row = c.fetchone()
                    break # quit() # strangely reacts much later
                    """

                    # TODO: detect disappearance immediately, using additional temp table

                    c.execute('UPDATE ' + namespace + '''.places SET disappeared = CAST ( GETDATE() as DATETIME )
                              WHERE disappeared IS NULL AND uid = ? AND (name = ? OR spot = 0) AND cityguid = ? AND (lat != ? OR lng != ?)''',
                              (place["uid"], name, currcityguid, place["lat"], place["lng"]))
                    c.execute('INSERT INTO ' + namespace + '''.places (uid, name, bike, spot, cityguid, lat, lng, created) SELECT ?,?,?,?,?,?,?,CAST ( GETDATE() as DATETIME )
                          WHERE NOT EXISTS (SELECT 1 FROM ''' + namespace + '.places WHERE uid = ? AND (name = ? OR spot = 0) AND cityguid = ? AND lat = ? AND lng = ? AND disappeared IS NULL)',
                          (place["uid"], name, bike, spot, currcityguid, place["lat"], place["lng"],
                           place["uid"], name, currcityguid, place["lat"], place["lng"])) # TODO: verify duplicate using bike & spot as well
                    if c.rowcount != 1:
                        print ('INSERT failed: ', place["uid"], name, bike, spot, currcityguid)
                    else:
                        currplaceguid = c.execute('SELECT @@IDENTITY').fetchone()[0]
                        # print ('currplaceguid(2): ', currplaceguid)
                c.execute('INSERT INTO ' + namespace + '''.#tmp_places (uid, cityguid) SELECT ?,?
                          WHERE NOT EXISTS (SELECT 1 FROM ''' + namespace + '.#tmp_places WHERE uid = ? AND cityguid = ?)', # duplicates should never happen
                          (place["uid"], currcityguid,
                           place["uid"], currcityguid)) # TODO: should I care about spot/bike here?

                for bike in place['bike_list']:
                    # print ('JSON bike: ', bike['number'])
                    # bikeuid = int(str(place['uid']) + str(bike['number'])) # Integers in Python 3 are of unlimited size.
                    rows = c.execute('SELECT guid FROM ' + namespace + '.bike_list WHERE number = ? AND placeguid = ? AND disappeared IS NULL',
                                             (bike['number'], currplaceguid)).fetchall()
                    if len(rows) > 1:
                        print ('Too many entries for ', bike['number'], currplaceguid, ' : ', len(rows))
                    elif len(rows) == 1:
                        currentnumber = rows[0][0] # value not used other than for testing
                        # print ('currentnumber: ', currentnumber)
                    else:
                        # disappear the bike from the old location, unless already gone:
                        c.execute('UPDATE ' + namespace + '''.bike_list SET disappeared = CAST ( GETDATE() as DATETIME )
                                  WHERE disappeared IS NULL AND number = ? AND placeguid != ?''', (bike['number'], currplaceguid))
                        # Insert the bike into the new location:
                        c.execute('INSERT INTO ' + namespace + '''.bike_list (number, placeguid, appeared, disappeared) SELECT ?,?,CAST ( GETDATE() as DATETIME ), NULL
                                  WHERE NOT EXISTS (SELECT 1 FROM ''' + namespace + '.bike_list WHERE number = ? AND placeguid = ? AND disappeared IS NULL)',
                                  (bike['number'], currplaceguid, # int(time.time()),
                                   bike['number'], currplaceguid))
                    c.execute('INSERT INTO ' + namespace + '''.#tmp_bike_list (number, placeguid) SELECT ?,?
                              WHERE NOT EXISTS (SELECT 1 FROM ''' + namespace + '.#tmp_bike_list WHERE number = ? AND placeguid = ?)', # duplicates should never happen
                              (bike['number'], currplaceguid, bike['number'], currplaceguid))

    # Places may disappear - in particular when they are bikes which can be left anywhere
    c.execute('UPDATE ' + namespace + '''.places SET disappeared = CAST ( GETDATE() as DATETIME ) WHERE disappeared IS NULL AND uid NOT IN
                 (SELECT ''' + namespace + '.#tmp_places.uid FROM ' + namespace + '.#tmp_places JOIN ' + namespace + '''.places
                  ON (#tmp_places.cityguid = places.cityguid))''')

    # mark as dispperad bikes which do not apppear in the current JSON anymore - presumable under way
    c.execute('UPDATE ' + namespace + '''.bike_list SET disappeared = CAST ( GETDATE() as DATETIME ) WHERE disappeared IS NULL AND number NOT IN
                 (SELECT ''' + namespace + '.#tmp_bike_list.number FROM ' + namespace + '.#tmp_bike_list JOIN ' + namespace + '''.bike_list
                  ON (#tmp_bike_list.placeguid = bike_list.placeguid))''')

    cnxn.commit()
    c.close()
    del c
    cnxn.close()     #<--- Close the connection
    del cnxn
    return

'''
SELECT * FROM NextBscraper.places ORDER BY created DESC;
SELECT COUNT(*) FROM NextBscraper.places;
'''
if dbType == DBType.AZURE:
    update_azure_tables()


# ## Count items in Azure DB

# In[ ]:


if dbType == DBType.AZURE:
    connectstr = get_azure_connect_str()
    cnxn = pyodbc.connect(connectstr)
    c = cnxn.cursor()

    print (c.execute('''SELECT (
                        SELECT COUNT(guid) FROM ''' + namespace + '''.countries ) AS Countries, 
                       (SELECT COUNT (guid) FROM ''' + namespace + '''.cities) AS Cities,
                       (SELECT COUNT (guid) FROM ''' + namespace + '''.places) AS Places,
                       (SELECT COUNT (guid) FROM ''' + namespace + '''.bike_list) AS Bikes''').fetchone())
                      # (SELECT COUNT (number) FROM ''' + namespace + '''.#tmp_bike_list) AS Tmp_Bikes''',

    c.close()
    del c
    cnxn.close()     #<--- Close the connection
    del cnxn

if dbType == DBType.HXE:
    cnxn = get_hxe_connection() # connection
    c = cnxn.cursor()

    c.execute('''SELECT * FROM (
                        SELECT COUNT(guid) AS Countries FROM ''' + namespace + '''.countries), 
                       (SELECT COUNT (guid) AS Cities FROM ''' + namespace + '''.cities),
                       (SELECT COUNT (guid) AS Places FROM ''' + namespace + '''.places),
                       (SELECT COUNT (guid) AS Bikes FROM ''' + namespace + '''.bike_list)''')
    print (c.fetchone())
                      # (SELECT COUNT (number) FROM ''' + namespace + '''.#tmp_bike_list) AS Tmp_Bikes''',

    c.close()
    del c
    cnxn.close()     #<--- Close the connection
    del cnxn


# ## Transfer data from JSON/dict to Sqlite DB (if applicable)

# In[ ]:


import time

def update_sqlite_tables():
    conn = sqlite3.connect(sqlitefile)

    c = conn.cursor()

    for country in nextbike_dict['countries']:
        # print (country["country"], country["country_name"])
        # INSERT OR IGNORE INTO bookmarks(users_id, lessoninfo_id) VALUES(123, 456) ; INSERT OR REPLACE geht auch
        # c.execute("INSERT INTO countries VALUES (?,?,?,?)", (country["country"], country["country_name"],int(time.time()),0))
        c.execute('''INSERT INTO countries (country, country_name, created) SELECT ?,?,?
                     WHERE NOT EXISTS (SELECT 1 FROM countries WHERE country = ? AND country_name = ?)''',
                  (country["country"], country["country_name"], int(time.time()),
                   country["country"], country["country_name"]))
        currcountryguid = c.execute("SELECT guid FROM countries WHERE country = ? AND country_name = ?",
                                    (country["country"], country["country_name"])).fetchone()[0] # SELECT last_insert_rowid(),
        # print (currcountryguid)
        # c.execute("UPDATE countries SET country = 'DP' WHERE country = 'DE'") # provoking changed entries for test purpose
        # print(country["cities"])
        for city in country['cities']:
            # print (city["uid"], city["name"])
            c.execute('''INSERT INTO cities (uid, name, countryguid, created) SELECT ?,?,?,?
                      WHERE NOT EXISTS (SELECT 1 FROM cities WHERE uid = ? AND name = ? AND countryguid = ?)''',
                      (city["uid"], city["name"], currcountryguid, int(time.time()),
                       city["uid"], city["name"], currcountryguid))
            # lastcityguid = c.execute("select seq from sqlite_sequence WHERE name = 'cities'").fetchone()[0]
            currcityguid = c.execute("SELECT guid FROM cities WHERE uid = ? AND name = ? AND countryguid = ?",
                                     (city["uid"], city["name"], currcountryguid)).fetchone()[0]
            # print(nextbike_dict['countries'][0]["cities"][0]['places'])
            for place in city['places']:
                # print (place['uid'], place['name'])
                c.execute('''INSERT INTO places (uid, name, cityguid, created) SELECT ?,?,?,?
                          WHERE NOT EXISTS (SELECT 1 FROM places WHERE uid = ? AND name = ? AND cityguid = ?)''',
                          (place["uid"], place["name"], currcityguid, int(time.time()),
                           place["uid"], place["name"], currcityguid))
                currplaceguid = c.execute("SELECT guid FROM places WHERE uid = ? AND name = ? AND cityguid = ?",
                                         (place["uid"], place["name"], currcityguid)).fetchone()[0]
                for bike in place['bike_list']:
                    # print (bike['number'])
                    # bikeuid = int(str(place['uid']) + str(bike['number'])) # Integers in Python 3 are of unlimited size. 
                    c.execute('''INSERT INTO bike_list (number, placeguid, appeared, disappeared) SELECT ?,?,?, NULL
                              WHERE NOT EXISTS (SELECT 1 FROM bike_list WHERE number = ? AND placeguid = ?)''',
                              (bike['number'], currplaceguid, int(time.time()),
                               bike['number'], currplaceguid)) # TODO: weakness: wouldn't support a bike going away and coming back to the same place
                    c.execute('''INSERT INTO tmp_bike_list (number, placeguid) SELECT ?,?
                              WHERE NOT EXISTS (SELECT 1 FROM tmp_bike_list WHERE number = ? AND placeguid = ?)''', # duplicates should never happen
                              (bike['number'], currplaceguid, bike['number'], currplaceguid))

    c.execute('''UPDATE bike_list SET disappeared = ? WHERE disappeared IS NULL AND number NOT IN
                 (SELECT tmp_bike_list.number FROM tmp_bike_list JOIN bike_list USING(placeguid))''',
              (int(time.time()), ))
    '''
    SELECT * FROM elections WHERE election_id NOT IN (
        SELECT elections.election_id from elections
        JOIN votes USING(election_id)
        WHERE votes.user_id='x'
    )
    '''

    conn.commit()

    """
    """
    for row in c.execute('SELECT * FROM countries ORDER BY country'):
      print (row)
    for row in c.execute('SELECT * FROM cities ORDER BY countryguid, name'):
      print (row)
    for row in c.execute('SELECT * FROM places ORDER BY cityguid, name'):
      print (row)
    print (c.execute('''SELECT COUNT(DISTINCT c.country) AS Countries, COUNT (DISTINCT ct.uid) AS Cities, COUNT (DISTINCT p.uid) AS Places
                        FROM countries c, cities ct, places p''').fetchone())
    for row in c.execute('SELECT * FROM bike_list ORDER BY placeguid, number'):
      print (row)

    print (c.execute('''SELECT (SELECT COUNT(guid) FROM countries ) AS Countries, 
                               (SELECT COUNT (guid) FROM cities) AS Cities,
                               (SELECT COUNT (guid) FROM places) AS Places,
                               (SELECT COUNT (guid) FROM bike_list) AS Bikes''').fetchone())
    

    conn.close()
    del conn
    return

if dbType == DBType.SQLITE:
    update_sqlite_tables()


# In[ ]:


# !ls -ltra
import os
relpath = "./DB/" # including trailing / please
for file in os.listdir(relpath):
    print (file, ': ', os.stat(relpath + file).st_size)


# ## Setup and Initialization
# 
# # Careful !!!

# In[ ]:


if False and dbType == DBType.AZURE: # To execute change to True but then be careful!
    connectstr = get_azure_connect_str()
    cnxn = pyodbc.connect(connectstr)
    c = cnxn.cursor()
    # c.execute('CREATE SCHEMA ' + namespace)

    c.execute('DROP TABLE IF EXISTS ' + namespace + '.#tmp_bike_list')
    c.execute('DROP TABLE IF EXISTS ' + namespace + '.bike_list')
    c.execute('DROP TABLE IF EXISTS ' + namespace + '.#tmp_places')
    c.execute('DROP TABLE IF EXISTS ' + namespace + '.places')
    c.execute('DROP TABLE IF EXISTS ' + namespace + '.cities')
    c.execute('DROP TABLE IF EXISTS ' + namespace + '.countries')

    cnxn.commit()
    c.close()
    del c
    cnxn.close()     #<--- Close the connection
    del cnxn
    print ('Tables dropped in schema ', namespace)
else:
    print ('To execute change to True but then be careful with schema: ', namespace)
    

