#!/bin/python
import sys,os
import logging
import pynfdump
import Flow
import datetime

logging.basicConfig(format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s')

def search(ip_list):

    middle_print = [False,False,False,False]#Prior one is middlefile, latter one is middlefile without those without IN or OUT
    middle_process = True

    if middle_print[0]:
        fout0 = open('middlefile_total.txt','w')
    if middle_print[1]:
        fout1 = open('middlefile_only_O','w')
    if middle_print[2]:
        fout2 = open('middlefile_not_equal','w')
    if middle_print[3]:
        fout3 = open('middlefile_equal','w')

    logger = logging.getLogger()
    hdlr = logging.StreamHandler()
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)

    InnerIP_list = ['166.111','59.66','101.5','101.6','183.172','183.173','118.229','202.38','202.112']
    ipdict_dict = {}
    datain = pynfdump.Dumper() 
    datain.set_where(start=None,end=None,filename='/lrjapps/netflowdata/datasource/flowdata/nfcapd.201705171800')
    query = ""
    for ip in ip_list:
        query = query + ' or host ' + ip
    query = 'proto icmp and (' + query[4:] + ')'
    logger.debug(query)
    records = datain.search(query)
    for r in records:
        af = r['af']
        first = r['first']
        last = r['last']
        msec_first = r['msec_first']
        msec_last = r['msec_last']
        proto = r['prot']
        srcip = str(r['srcip'])
        srcport = r['srcport']
        dstip = str(r['dstip'])
        dstport = r['dstport']
        srcas = r['srcas']
        dstas = r['dstas']
        packets = r['packets']
        tbytes = r['bytes']
        srcip_prefix = srcip.split('.')[0] + '.' + srcip.split('.')[1]
        dstip_prefix = dstip.split('.')[0] + '.' + dstip.split('.')[1]

        if srcip in ip_list and dstip_prefix in InnerIP_list :
            flag = 'IN'
        elif dstip in ip_list and srcip_prefix in InnerIP_list:
            flag = 'OUT'
        elif srcip not in ip_list and dstip not in ip_list:
            logging.error(srcip + ' ' + dscip + ' UNEXCEPTED source/destination address')
            continue
        else:#This means srcip or dstip is in ip_list, but dstip/srcip is located out of campus
            continue
        
        if flag == 'IN':
            key = (dstip,srcip)#Tsinghua IP is [0] and target IP is [1]
        else:
            key = (srcip,dstip)

        if key not in ipdict_dict:
            ipdict_dict[key] = {'IN':[],'OUT':[]}
           
        if flag == 'IN':
            ipdict_dict[key]['IN'].append(Flow.IcmpFlow(srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes,flag))
        else:
            ipdict_dict[key]['OUT'].append(Flow.IcmpFlow(srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes,flag))

    if middle_print[0]:
        for key in ipdict_dict:
            prtln = key[0] + ' => '+ key[1]  + '\n' + '\tIN:\n'
            fout0.write(prtln)
            for item in ipdict_dict[key]['IN']:
                fout0.write('\t\t'+item.display_string()+'\n')
            fout0.write('\tOUT:\n')
            for item in ipdict_dict[key]['OUT']:
                fout0.write('\t\t'+item.display_string()+'\n')
            fout0.write('\n')
        fout0.close()

    if middle_print[1]:
        for key in ipdict_dict:
            if len(ipdict_dict[key]['IN']) == 0:
                prtln = key[0] + ' => '+ key[1]  + '\n' + '\tOUT:\n'
                fg = 0
                for item in ipdict_dict[key]['OUT']:
                    if item.display_dict()['type'] == 'REQUEST_OUT':
                        if fg == 0:
                            fout1.write(prtln)
                            fg = 1
                        fout1.write('\t\t'+item.display_string()+'\n')
                fout1.write('\n')
        fout1.close()

    if middle_print[2]:
        for key in ipdict_dict:
            if len(ipdict_dict[key]['IN']) == len(ipdict_dict[key]['OUT']) or len(ipdict_dict[key]['IN']) == 0:
                continue
            prtln = key[0] + ' => '+ key[1]  + '\n' + '\tIN:\n'
            fout2.write(prtln)
            for item in ipdict_dict[key]['IN']:
                fout2.write('\t\t'+item.display_string()+'\n')
            fout2.write('\tOUT:\n')
            for item in ipdict_dict[key]['OUT']:
                fout2.write('\t\t'+item.display_string()+'\n')
            fout2.write('\n')
        fout2.close()

    if middle_print[3]:
        for key in ipdict_dict:
            if len(ipdict_dict[key]['IN']) != len(ipdict_dict[key]['OUT']):
                continue
            prtln = key[0] + ' => '+ key[1]  + '\n' + '\tIN:\n'
            fout3.write(prtln)
            for item in ipdict_dict[key]['IN']:
                fout3.write('\t\t'+item.display_string()+'\n')
            fout3.write('\tOUT:\n')
            for item in ipdict_dict[key]['OUT']:
                fout3.write('\t\t'+item.display_string()+'\n')
            fout3.write('\n')
        fout3.close()

    if middle_process:
        middle_process_f(ipdict_dict)

def middle_process_f(d):
    time_inter = datetime.timedelta(0,2,0)

    fin = open('/lrjapps/netflowdata/datasource/ICMPdata/times/18:00:00.txt','r')

    result_dict = {}
    for key in d:
        if key[1] not in result_dict:
            result_dict[key[1]] = {'ICMP':[],'NETFLOW':{'average':0,'data':{},'flow':[]}}

    for key in d:
        length = len(d[key]['IN'])
        if length != len(d[key]['OUT']):
            continue
        for i in range(length):
            timed_first = d[key]['IN'][i].first - d[key]['OUT'][i].first
            packetd = d[key]['IN'][i].packets - d[key]['OUT'][i].packets
            if timed_first > datetime.timedelta(0,30,0) or timed_first < datetime.timedelta(0,0,0) or packetd != 0:
                continue

            if key[0] not in result_dict[key[1]]['NETFLOW']['data']:
                result_dict[key[1]]['NETFLOW']['data'][key[0]] = [timed_first]
            else:
                result_dict[key[1]]['NETFLOW']['data'][key[0]].append(timed_first)

            result_dict[key[1]]['NETFLOW']['flow'].append((d[key]['IN'][i],d[key]['OUT'][i]))

    for key in result_dict:
        logging.debug(key,result_dict[key]['NETFLOW']['data'])
        count = 0
        summ = datetime.timedelta(0,0,0)
        for key0 in result_dict[key]['NETFLOW']['data']:
            count += len(result_dict[key]['NETFLOW']['data'][key0])
            for dld in result_dict[key]['NETFLOW']['data'][key0]:
                summ += dld
        try:
            result_dict[key]['NETFLOW']['average'] = summ.total_seconds()/count*1000
        except ZeroDivisionError:
            pass


    line = fin.readline()
    while line:
        ip = line.split('#')[0].strip()
        items = line.split('#')[1].split(',')
#        fmt = "%Y-%m-%d %H:%M:%S"
#        dd = datetime.datetime(2000,1,1,0,0,0)
        
#        time = dd.strptime(items[0],fmt)
        time = items[0]
        rttavg = items[1]
        rttmin = items[2]
        rttmax = items[3]
        try:
            result_dict[ip]['ICMP'] = [time,rttavg,rttmin,rttmax]
        except KeyError:
            pass
        line = fin.readline()

#    for key in result_dict:
#        print key, result_dict[key]['ICMP'],result_dict[key]['NETFLOW']['data']
    for key in result_dict:
        if not result_dict[key]['NETFLOW']['data']:
            continue
        println = [key,str(result_dict[key]['NETFLOW']['average'])]+result_dict[key]['ICMP']
        print ' '.join(println)



        

            

