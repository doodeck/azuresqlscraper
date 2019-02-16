
import json
import psutil
import requests

# TODO: protect against double execution (lock file)

# interesting how much memory do we need for this kind of processing
print("before: ", psutil.virtual_memory())
r = requests.get('https://nextbike.net/maps/nextbike-live.json') # ?place=4728239
# print (type(r.json())) <class 'dict'>
# print (r.json())
nextbike_dict = r.json()
'''
with open(localfiles[0]) as f:
    nextbike_dict = json.load(f)
'''
print("after: ", psutil.virtual_memory())


print(nextbike_dict.keys())
countrynodescnt = len(nextbike_dict['countries'])
print(countrynodescnt)
if countrynodescnt < 100:
  quit()

# print(nextbike_dict['countries'][0])
# print(nextbike_dict['countries'][0]["cities"])
# print(nextbike_dict['countries'][0]["cities"][0]['places'])
# print(nextbike_dict['countries'][0]['cities'][0]['places'][0]['bike_list'])

sqlitefile = "./DB/database.sqlite"

import sqlite3
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


# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

import time

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
"""

print (c.execute('''SELECT (SELECT COUNT(guid) FROM countries ) AS Countries, 
                           (SELECT COUNT (guid) FROM cities) AS Cities,
                           (SELECT COUNT (guid) FROM places) AS Places,
                           (SELECT COUNT (guid) FROM bike_list) AS Bikes''').fetchone())

conn.close()

# !ls -ltra
import os
relpath = "./DB/" # including trailing / please
for file in os.listdir(relpath):
    print (file, ': ', os.stat(relpath + file).st_size)
