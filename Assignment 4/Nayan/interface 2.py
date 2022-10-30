import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    result = []
    cur = openconnection.cursor()

    partSelectQuery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={} AND minrating<={};'''.format(ratingMinValue, ratingMaxValue)
    cur.execute(partSelectQuery)
    parts = cur.fetchall()
    parts = [part[0] for part in parts]

    rangeselectquery = '''SELECT * FROM rangeratingspart{} WHERE rating>={} and rating<={};'''

    for part in parts:
        cur.execute(rangeselectquery.format(part, ratingMinValue, ratingMaxValue))
        sqlres = cur.fetchall()
        for res in sqlres:
            res = list(res)
            res.insert(0,'RangeRatingsPart{}'.format(part))
            result.append(res)

    roundrobincountquery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    cur.execute(roundrobincountquery)
    roundrobinparts = cur.fetchall()[0][0]

    roundrobinselectquery = '''SELECT * FROM roundrobinratingspart{} WHERE rating>={} and rating<={};'''

    for i in range(0,roundrobinparts):
        cur.execute(roundrobinselectquery.format(i, ratingMinValue, ratingMaxValue))
        sqlres = cur.fetchall()
        for res in sqlres:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            result.append(res)

    writeToFile('RangeQueryOut.txt', result)




def PointQuery(ratingsTableName, ratingValue, openconnection):
    result = []
    cur = openconnection.cursor()

    partSelectQuery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={} AND minrating<={};'''.format(ratingValue,ratingValue)
    cur.execute(partSelectQuery)
    parts = cur.fetchall()
    parts = [part[0] for part in parts]

    rangeselectquery = '''SELECT * FROM rangeratingspart{} WHERE rating={};'''

    for part in parts:
        cur.execute(rangeselectquery.format(part, ratingValue))
        sqlres = cur.fetchall()
        for res in sqlres:
            res = list(res)
            res.insert(0, 'RangeRatingsPart{}'.format(part))
            result.append(res)

    roundrobincountquery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''

    cur.execute(roundrobincountquery)
    roundrobinparts = cur.fetchall()[0][0]

    roundrobinselectquery = '''SELECT * FROM roundrobinratingspart{} WHERE rating={};'''

    for i in range(0, roundrobinparts):
        cur.execute(roundrobinselectquery.format(i, ratingValue))
        sqlres = cur.fetchall()
        for res in sqlres:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            result.append(res)

    writeToFile('PointQueryOut.txt', result)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()