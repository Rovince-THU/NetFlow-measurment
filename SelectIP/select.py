#!/bin/python
import sys,os
import logging
import pynfdump
import datetime

InnerIP_list = ['166.111','59.66','101.5','101.6','183.172','183.173','118.229','202.112','202.38','106.120']

datain = pynfdump.Dumper('../../../',profile='datasource/16',sources=['nfcapd.201711161000','nfcapd.201711161005']) 
#datain.set_where(start=None,end=None,filename='../datasource/flowdata/nfcapd.201705171805')
datain.set_where(start=None,end=None)
query = "proto icmp and host 166.111.8.241"

record = datain.search(query)

for r in record:
    af = r['af']
    first = r['first']
    last = r['last']
    msec_first = r['msec_first']
    msec_last = r['msec_last']
    proto = r['prot']
    srcip = str(r['srcip'])
    dstip = str(r['dstip'])
    srcport = r['srcport']
    dstport = r['dstport']
    srcas = r['srcas']
    dstas = r['dstas']
    packets = r['packets']
    tbytes = r['bytes']
    srcip_prefix = srcip.split('.')[0] + '.' + srcip.split('.')[1]
    dstip_prefix = dstip.split('.')[0] + '.' + dstip.split('.')[1]
    first_time = first + datetime.timedelta(microseconds = msec_first)
    last_time = first + datetime.timedelta(microseconds = msec_last)

prtstr = srcip + ' => ' + dstip + ':' + dstport + ' '+ first_time + ' ' + packets + ' ' + tbytes
print prtstr

    


