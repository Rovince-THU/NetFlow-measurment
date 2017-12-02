#!/bin/python

import datetime
import logging

#logging.basicCofing(level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s')

timelist = []
for i in range(10,24):
    timelist.append(str(i)+':00:00')
    timelist.append(str(i)+':30:00')

timeoutputlist = []
datetimelist = []
IPlist = []

InnerIP_list = ['166.111','59.66','101.5','101.6','183.172','183.173','118.229','202.38','202.112']
fin = open('/data2/datasource/ICMP/active_measurement_201711.txt','r')

for time in timelist:
    timestrl = '2017-11-16 '+time
    timeoutputlist.append('/data2/datasource/ICMP/time/'+ time + '.txt')
    datetimelist.append(datetime.datetime.strptime(timestrl,'%Y-%m-%d %H:%M:%S'))

#print datetimelist[8]

for time in timelist:
    IPlist.append({})

line = fin.readline()
line = fin.readline()
while line:
    line = fin.readline()
    try:
        items = line.split('|')
        time = items[5].strip()
        dstIP = items[4].strip()
        rttavg = items[6].strip()
        rttmax = items[8].strip()
        rttmin = items[7].strip()
        timeTuple = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

        IPdetails = dstIP.split('.')
        IPprefix = '.'.join(IPdetails[0:2])
        if IPprefix in InnerIP_list:
            continue

        flag = -1
        for i in range(len(timelist)):
            if timeTuple > datetimelist[i]:
                continue
            if timeTuple < datetimelist[i]:
                continue
            flag = i
            break

        if flag < 0:
            continue

        i = flag
        if dstIP in IPlist[i]:
            if (time,rttavg,rttmin,rttmax) != IPlist[i][dstIP][0]:
                IPlist[i][dstIP].append((time,rttavg,rttmin,rttmax))
        if dstIP not in IPlist[i]:
            IPlist[i][dstIP] = [(time,rttavg,rttmin,rttmax)]

    except IndexError:
        fin.close()
        break

for i in range(len(IPlist)):
    foutstr = timeoutputlist[i]
    fout = open(foutstr,'w')
    for ip in IPlist[i]:
        prtline = [ip]
        for item in IPlist[i][ip]:
            prtline.append(','.join(item))
        fout.write('#'.join(prtline)+'\n')

    fout.close()



        

