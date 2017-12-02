#!/bin/python
import sys,os
import logging
import pynfdump
import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s')
InnerIP_list = ['166.111','59.66','101.5','101.6','183.172','183.173','118.229','202.112','202.38','106.120']

datain = pynfdump.Dumper('../',profile='datasource/flowdata',sources='flowdata/') 
#datain.set_where(start=None,end=None,filename='../datasource/flowdata/nfcapd.201705171805')
datain.set_where(start=None,end=None)
query = "proto icmp"

fin = open("ip.txt",'r')
line = fin.readline()
ip_dict = {}
while line:
    ip_dict[line.strip()] = 0
    line = fin.readline()
#    for ip in ip_list:
#        query = query + ' host or ' + ip
#    query = query[1:] + ' and proto icmp'
logging.debug(query)
records = datain.search(query)
countii = 0
countio = 0
countoo = 0
countleft = 0
proto_list = []
for r in records:
    first = r['first']
    msec_first = r['msec_first']
    last = r['last']
    msec_last = r['msec_last']
    first_time = first + datetime.timedelta(microseconds = msec_first*1000)
    last_time = last + datetime.timedelta(microseconds = msec_last*1000)
    proto = r['prot']
    srcip = str(r['srcip'])
    srcport = r['srcport']
    dstip = str(r['dstip'])
    dstport = r['dstport']
    packets = r['packets']
    byte = r['bytes']
    srcip_prefix = srcip.split('.')[0] + '.' + srcip.split('.')[1]
    dstip_prefix = dstip.split('.')[0] + '.' + dstip.split('.')[1]

    flag = False

    if dstip_prefix in InnerIP_list and srcip in ip_dict:
        flag = True

    if srcip_prefix in InnerIP_list and dstip in ip_dict:
        flag = True

    if srcip in ip_dict:
        ip_dict[srcip] += 1
    elif dstip in ip_dict:
#        prtln = srcip +':'+ str(srcport) + ' => ' + dstip +':'+ str(dstport) + ' ' + first_time.strftime("%H:%M:%S:%f") + ' ' + last_time.strftime("%H:%M:%S:%f") + ' ' + str(packets) + ' ' + str(byte)
#        print prtln
        ip_dict[dstip] += 1

ip_dict_sorted = sorted(ip_dict.iteritems(), key = lambda asd:asd[1], reverse = True)

for key in ip_dict_sorted:
    print key[0],key[1]
    
#    if srcip_prefix in InnerIP_list and dstip_prefix in InnerIP_list:
#        countii += 1
#    elif srcip_prefix not in InnerIP_list and dstip_prefix not in InnerIP_list:
#        countoo += 1
#        if srcip_prefix not in anlist and dstip_prefix not in anlist:
#            countleft += 1
#            fout.write(''.join([rtime.strftime('%H:%M:%S.%f')[:-3],'   ',srcip,':',str(srcport),' => ',str(dstip),':',str(dstport)])+'\n')
#    else:
#        countio += 1


#logging.debug(srcip_prefix,dstip_prefix)
#fout.close()



