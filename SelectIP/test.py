import pynfdump
import datetime
import Flow

ip_dict = {}
agg_dict = {}

fin = open('input.txt','r')
lines = fin.readlines()

string = lines[0].strip().split('#')
key = string[0]
ze = key
ip_dict[key] = {'IN':[],'OUT':[],'ICMP':[string[1],string[2],string[3].strip()]}

agg_dict[key]= {\
    'IN':[],\
    'OUT':[],\
    'ICMP':[string[1],string[2],string[3].strip()],\
    'time_itv':-3,\
    'flow_time_error_in':[0,0],\
    'flow_time_error_out':[0,0],\
    'first_time_error':False,\
    'last_time_error':False }

for i in range(1,13):
    r = lines[i].strip().split('#')
    srcip = r[0]
    srcport = 0
    dstip = r[1].split(':')[0]
    dstport = r[1].split(':')[1]

    f = r[2]

    ymd = f.split(' ')[0].split('-')
    year = int(ymd[0])
    month = int(ymd[1])
    day = int(ymd[2])
    hms = f.split(' ')[1].split('.')[0].split(':')
    hour = int(hms[0])
    minite = int(hms[1])
    second = int(hms[2])
    msec_first = int(f.split('.')[1][:-3])
    first = datetime.datetime(year,month,day,hour,minite,second)

#    first = datetime.datetime(f.split('-')[0],f.split('-')[1],f.split(' ')[0].split('-')[2],f.split(' ')[1].split[':'][0],f.split(' ')[1].split(':')[1],f.split(' ')[1].split(':')[2].split('.')[0],f.split('.')[1])
    l = r[3]
#    last = datetime.datetime(l.split('-')[0],l.split('-')[1],l.split(' ')[0].split('-')[2],l.split(' ')[1].split[':'][0],l.split(' ')[1].split(':')[1],l.split(' ')[1].split(':')[2].split('.')[0],l.split('.')[1])
    ymd = l.split(' ')[0].split('-')
    year = int(ymd[0])
    month = int(ymd[1])
    day = int(ymd[2])
    hms = l.split(' ')[1].split('.')[0].split(':')
    hour = int(hms[0])
    minite = int(hms[1])
    second = int(hms[2])
    msec_last = int(l.split('.')[1][:-3])
    last = datetime.datetime(year,month,day,hour,minite,second)

    packets = int(r[4])
    tbytes = int(r[5])

   
    if dstip in ip_dict:
        flag = 'OUT'
    elif srcip in ip_dict:
        flag = 'IN'
    else:
        continue

    ip_dict[key][flag].append(Flow.IcmpFlow(srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes,flag))

for key in ip_dict:
    if len(ip_dict[key]['IN']) == 0:
        agg_dict[key]['time_itv'] = -2
        continue
    if len(ip_dict[key]['OUT']) == 0:
        agg_dict[key]['time_itv'] = -2
        continue
    #Here to gathering the flows 
    ip = key
    srcip, srcport = ip_dict[key]['OUT'][0].get_srcip()
    dstip, dstport = ip_dict[key]['OUT'][0].get_dstip()
    lgin = len(ip_dict[key]['IN'])
    lgout = len(ip_dict[key]['OUT'])
    real_flow_dict = {'IN':[],'OUT':[]}
    real_flow_list = []
    real_flow_dict['IN'] = [Flow.FlowGroup(dstip,srcport,srcip,0)]
    real_flow_dict['OUT'] = [Flow.FlowGroup(srcip,srcport,dstip,dstport)]
    i = 0
#    print lgout
    for j in range(lgout):
        agg_dict[key]['flow_time_error_out'][1] += 1
        if ip_dict[key]['OUT'][j].get_first_time() > ip_dict[key]['OUT'][j].get_last_time():
            agg_dict[key]['flow_time_error_out'][0] += 1

        if real_flow_dict['OUT'][i].add_flow(ip_dict[key]['OUT'][j]):
            pass
#            print i
#            print j,i,True
#            print real_flow_dict['OUT'][i].display_string()
        else:
#            print j,i,False
#            print real_flow_dict
            i += 1
            real_flow_dict['OUT'].append(Flow.FlowGroup(srcip,0,dstip,2048))
            real_flow_dict['OUT'][i].add_flow(ip_dict[key]['OUT'][j])
#            except TypeError:
#                print key,ip_dict[key]
#                print j,ip_dict[key]['OUT'][j]
#                exit()

    i = 0
    for j in range(lgin):
        agg_dict[key]['flow_time_error_in'][1] += 1
        if ip_dict[key]['IN'][j].get_first_time() > ip_dict[key]['IN'][j].get_last_time():
            agg_dict[key]['flow_time_error_in'][0] += 1

        if real_flow_dict['IN'][i].add_flow(ip_dict[key]['IN'][j]):
            pass
        else:
            i += 1
            real_flow_dict['IN'].append(Flow.FlowGroup(dstip,0,srcip,0))
            real_flow_dict['IN'][i].add_flow(ip_dict[key]['IN'][j])

    for item in real_flow_dict['OUT']:
        agg_dict[key]['OUT'].append(item)

    for item in real_flow_dict['IN']:
        agg_dict[key]['IN'].append(item)

    lenin = len(real_flow_dict['IN'])
    for item in real_flow_dict['OUT']:
        for i in range(lenin):
            if datetime.timedelta(0,-10,0)  < item.get_first_time() - real_flow_dict['IN'][i].get_first_time() < datetime.timedelta(0,10,0):
                real_flow_list.append({'IN':real_flow_dict['IN'][i],'OUT':item})
                break
        if i == range(lenin):
            pass

    for item in real_flow_list:
        if agg_dict[key]['time_itv'] in [-2,-4]:
            break
        elif agg_dict[key]['time_itv'] != -3:
            if item['OUT'].get_first_time() > datetime.datetime(2017,11,16,10,28,0):
                break
            else:
                agg_dict[key]['time_itv'] = -4
        time_itvf = item['IN'].get_first_time() - item['OUT'].get_first_time()
        time_itvl = item['IN'].get_last_time() - item['OUT'].get_last_time()
        dtzero = datetime.timedelta(0,0,0)
        if time_itvf < dtzero:
            agg_dict[key]['first_time_error'] = True
            if time_itvl < dtzero:
                agg_dict[key]['time_itv'] = -1
                agg_dict[key]['last_time_error'] = True
            else:
                agg_dict[key]['time_itv'] = time_itvl
        else:
            agg_dict[key]['time_itv'] = time_itvf
            if time_itvl < dtzero:
                agg_dict[key]['last_time_error'] = True

print ip_dict
print agg_dict
for item in ip_dict[ze]['IN']:
    print item.display_string()
print 
for item in ip_dict[ze]['OUT']:
    print item.display_string()
print
for item in agg_dict[ze]['IN']:
    print item.display_string()
print 
for item in agg_dict[ze]['OUT']:
    print item.display_string()
print
#for item in real_flow_dict['IN']:
#    print item.display_string()
#print
#for item in real_flow_dict['OUT']:
#    print item.display_string()


        
#
