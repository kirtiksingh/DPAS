#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    of = 'RangeQueryOut.txt'
    rmt = 'rangeratingsmetadata'
    rrmt = 'roundrobinratingsmetadata'

    command = "SELECT MAX(partitionnum) FROM %s" % rmt
    cur.execute(command)

    rrange = cur.fetchone()[0] + 1

    # if file exists, delete and recreate
    if os.path.exists(of):
        os.remove(of)

    for i in range(0, rrange):
        minCmd = "SELECT minrating FROM %s WHERE partitionnum = %s" % (rmt, str(i))
        maxCmd = "SELECT maxrating FROM %s WHERE partitionnum = %s" % (rmt, str(i))

        cur.execute(minCmd)
        min = cur.fetchone()[0]
        cur.execute(maxCmd)
        max = cur.fetchone()[0]

        if ratingMaxValue >= min and ratingMinValue <= max:
            rangeCmd = "SELECT * FROM rangeratingspart%s WHERE rating >= %s and rating <= %s" % (str(i), ratingMinValue, ratingMaxValue)
            cur.execute(rangeCmd)
            rangeresults = cur.fetchall()

            # write to file
            with open(of, 'a') as f:
                for r in rangeresults:
                    f.write("RangeRatingsPart%s,%s,%s,%s\n" % (i, r[0], r[1], r[2]))

    command = "SELECT partitionnum FROM %s" % (rrmt)
    cur.execute(command)
    robinRange = cur.fetchone()[0]

    for j in range(0, robinRange):
        cmd = "SELECT * FROM roundrobinratingspart%s WHERE rating >= %s AND rating <= %s" % (str(j), ratingMinValue, ratingMaxValue)
        cur.execute(cmd)
        robinresults = cur.fetchall()

        # write to file
        with open(of, 'a') as f:
            for r in robinresults:
                f.write("RoundRobinRatingsPart%s,%s,%s,%s\n" % (j, r[0], r[1], r[2]))


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    of = 'PointQueryOut.txt'
    rmt = 'rangeratingsmetadata'
    rrmt = 'roundrobinratingsmetadata'

    command = "SELECT MAX(partitionnum) FROM %s" % rmt
    cur.execute(command)

    rrange = cur.fetchone()[0] + 1

    # if file exists, delete and recreate
    if os.path.exists(of):
        os.remove(of)

    for i in range(0, rrange):
        minCmd = "SELECT minrating FROM %s WHERE partitionnum = %s" % (rmt, str(i))
        maxCmd = "SELECT maxrating FROM %s WHERE partitionnum = %s" % (rmt, str(i))

        cur.execute(minCmd)
        min = cur.fetchone()[0]
        cur.execute(maxCmd)
        max = cur.fetchone()[0]

        if ratingValue >= min and ratingValue <= max:
            rangeCmd = "SELECT * FROM rangeratingspart%s WHERE rating = %s" % (str(i), ratingValue)
            cur.execute(rangeCmd)
            prangeresults = cur.fetchall()

            # write to file
            with open(of, 'a') as f:
                for r in prangeresults:
                    f.write("RangeRatingsPart%s,%s,%s,%s\n" % (i, r[0], r[1], r[2]))

    command = "SELECT partitionnum FROM %s" % (rrmt)
    cur.execute(command)
    robinRange = cur.fetchone()[0]

    for j in range(0, robinRange):
        cmd = "SELECT * FROM roundrobinratingspart%s WHERE rating = %s" % (str(j), ratingValue)
        cur.execute(cmd)
        probinresults = cur.fetchall()

        # write to file
        with open(of, 'a') as f:
            for r in probinresults:
                f.write("RoundRobinRatingsPart%s,%s,%s,%s\n" % (j, r[0], r[1], r[2]))


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
