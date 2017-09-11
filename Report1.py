"""
#
#
"""
import mysql.connector
import dbconfig as cfg

# Configuration is read from dbconfig.py
cnx = mysql.connector.connect(host=cfg.mysql['host'], user=cfg.mysql['user'], 
                               password=cfg.mysql['password'], 
                               database=cfg.mysql['database'])

cursor = cnx.cursor()

query = ("SELECT raceId, carName, lap, lapTime FROM laps "
         "ORDER BY raceId, carName, lap")

cursor.execute(query)

# Heading
print()
print("******************************")
print("{:6} {:10} {:4} {:12}".format("raceId", "carName", "lap", "lapTime"))
print("******************************")
print()

for (raceId, carName, lap, lapTime) in cursor:
    print("{:6} {:10} {:4} {:12}".format(raceId, carName, str(lap), str(lapTime)))

print()

cursor.close()
cnx.close()
