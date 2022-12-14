#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    if(ratingMinValue < 0.0 or ratingMaxValue > 5.0):
        return

    outputList = []
    rangeMetaTable = 'rangeratingsmetadata'
    roundRobinMetaTable = 'roundrobinratingsmetadata'

    # delete and create new file, if it already exists
    if os.path.exists('RangeQueryOut.txt'):
        os.remove('RangeQueryOut.txt')

    with openconnection.cursor() as cursor:
        partitionQuery = '''SELECT partitionnum FROM {} WHERE maxrating>={} AND minrating<={};'''.format(rangeMetaTable, ratingMinValue, ratingMaxValue)
        cursor.execute(partitionQuery)
        partitions = cursor.fetchall()
        partitions = [partition[0] for partition in partitions]

        rangeSelectQuery = '''SELECT * FROM rangeratingspart{} WHERE rating>={} and rating<={};'''

        for partition in partitions:
            formattedRangeQuery = rangeSelectQuery.format(partition, ratingMinValue, ratingMaxValue)
            cursor.execute(formattedRangeQuery)
            values = cursor.fetchall()
            for val in values:
                val = list(val)
                val.insert(0,'RangeRatingsPart{}'.format(partition))
                outputList.append(val)

        rrCountQuery = '''SELECT partitionnum FROM {};'''.format(roundRobinMetaTable)
        cursor.execute(rrCountQuery)
        roundRobinParts = cursor.fetchall()[0][0]

        rrSelectQuery = '''SELECT * FROM roundrobinratingspart{} WHERE rating>={} and rating<={};'''

        for i in range(roundRobinParts):
            formattedRoundRobinQuery = rrSelectQuery.format(i, ratingMinValue, ratingMaxValue)
            cursor.execute(formattedRoundRobinQuery)
            values = cursor.fetchall()
            for val in values:
                val = list(val)
                val.insert(0, 'RoundRobinRatingsPart{}'.format(i))
                outputList.append(val)

        writeToFile('RangeQueryOut.txt', outputList)
        cursor.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):

    if(ratingValue > 5.0 or ratingValue < 0.0):
        return

    outputList = []
    rangeMetaTable = 'rangeratingsmetadata'
    roundRobinMetaTable = 'roundrobinratingsmetadata'

    # delete and create new file, if it already exists
    if os.path.exists('PointQueryOut.txt'):
        os.remove('PointQueryOut.txt')

    with openconnection.cursor() as cursor:

        partitionQuery = '''SELECT partitionnum FROM {} WHERE maxrating>={} AND minrating<={};'''.format(rangeMetaTable, ratingValue, ratingValue)
        cursor.execute(partitionQuery)
        partitions = cursor.fetchall()
        partitions = [partition[0] for partition in partitions]

        rangeSelectQuery = '''SELECT * FROM rangeratingspart{} WHERE rating={};'''

        for partition in partitions:
            formattedRangeQuery = rangeSelectQuery.format(partition, ratingValue)
            cursor.execute(formattedRangeQuery)
            values = cursor.fetchall()
            for val in values:
                val = list(val)
                val.insert(0, 'RangeRatingsPart{}'.format(partition))
                outputList.append(val)

        rrCountQuery = '''SELECT partitionnum FROM {};'''.format(roundRobinMetaTable)

        cursor.execute(rrCountQuery)
        roundRobinParts = cursor.fetchall()[0][0]

        rrSelectQuery = '''SELECT * FROM roundrobinratingspart{} WHERE rating={};'''

        for i in range(roundRobinParts):
            formattedRoundRobinQuery = rrSelectQuery.format(i, ratingValue)
            cursor.execute(formattedRoundRobinQuery)
            values = cursor.fetchall()
            for val in values:
                val = list(val)
                val.insert(0, 'RoundRobinRatingsPart{}'.format(i))
                outputList.append(val)

        writeToFile('PointQueryOut.txt', outputList)
        cursor.close()


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
