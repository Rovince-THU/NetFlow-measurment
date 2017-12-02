#!/bin/python
import sys
import datetime

timerangemin = datetime.datetime.strptime('2017-05-17 00:00:00', '%Y-%m-%d %H:%M:%S')
timerangemax = datetime.datetime.strptime('2017-05-17 23:00:00', '%Y-%m-%d %H:%M:%S')

fin = open('1.txt','r')
fout = open('0517.txt','w')
line = fin.readline()
while line:
    try:
        items = line.split(',')
        time = items[5]
        timetuple = datetime.datetime.strptime(time,'%Y-%m-%d %H:%M:%S')
        if timerangemin < timetuple < timerangemax:
            fout.write(line)
    except IndexError:
        break
    except ValueError:
        fout.write(line)
    finally:
        line = fin.readline()
