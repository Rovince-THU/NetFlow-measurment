import pynfdump
import datetime
import Flow

#d = pynfdump.Dumper('/data2/datasource/',profile='16/',sources=['nfcapd.201711161000','nfcapd.201711161005'])
d = pynfdump.Dumper()
#d.set_where(start=None,end=None,filename='/data2/datasource/16/nfcapd.201711161000')
dstring = '/data2/datasource/16/'
for i in range(6):
    nstr = str(i*5)
    if len(nstr) < 2:
        nstr = '0' + nstr
    dstring += 'nfcapd.2017111610' + nstr + ':'

dstring = dstring[:-1]

d.set_where(start=None,end=None,dirfiles='/data2/datasource/16/nfcapd.201711161000:nfcapd.201711161030')

records = d.search('proto icmp and host 166.111.8.241')

fin = open('/data2/datasource/ICMP/time/10:00:00.txt','r')
fout = open('./out.txt2','w')
ip_dict = {}
agg_dict = {}

for line in fin.readlines():
    items = line.split('#')
    ip = items[0].strip()
    string = items[1].split(',')
    if ip not in ip_dict:
        ip_dict[ip] = {'IN':[],'OUT':[],'ICMP':[string[1],string[2],string[3]]}
        agg_dict[i]] = {'IN':[],'OUT':[],'ICMP':[string[1],string[2],string[3]],'time_itv':-2,'flow_time_error_in':[0,0],'flow_time_error_out':[0,0],'fist_time_error':False,'last_time_error':False}

for r in records:
    first = r['first']
    last = r['last']
    msec_first = r['msec_first']
    msec_last = r['msec_last']
    srcip = str(r['srcip'])
    dstip = str(r['dstip'])
    srcport = r['srcport']
    dstport = r['dstport']
    packets = r['packets']
    tbytes = r['bytes']
    srcip_prefix = srcip.split('.')[0] + '.' + srcip.split('.')[1]
    dstip_prefix = dstip.split('.')[0] + '.' + dstip.split('.')[1]
#    first_time = first + datetime.timedelta(microseconds = msec_first)
#    last_time = first + datetime.timedelta(microseconds = msec_last)
    
    if dstip in ip_dict:
        key = dstip
        flag = 'OUT'
    elif srcip in ip_dict:
        key = srcip
        flag = 'IN'
    else:
        continue

#    prtstr = srcip + ' => ' + dstip + ':' + str(dstport) + ' '+ first_time.strftime("%Y-%m-%d %H:%M:%S.%f") + ' ' + last_time.strftime("%Y-%m-%d %H:%M:%S.%f")+ ' ' + str(packets) + ' ' + str(tbytes)
    ip_dict[key][flag].append(IcmpFlow(scrip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes))

for key in ip_dict:
    if len(ip_dict[key]['IN']) == 0:
        agg_dict['time_itv'] = -1
        continue
    #Here to gathering the flows 
    head_flow


for key in ip_dict:
    str1 = ' '.join(ip_dict[key]['ICMP'])
    fout.write(key+': '+str1+'\n'+'\tIN:\n')
    for item in ip_dict[key]['IN']:
        fout.write('\t\t'+item+'\n')
    fout.write('\n\tOUT:\n')
    for item in ip_dict[key]['OUT']:
        fout.write('\t\t'+item+'\n')
    fout.write('\n')

    if len(ip_dict[key]) == 0:
        count += 1

print count
