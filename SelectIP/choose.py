import pynfdump
import datetime
import Flow


def main():
    #d = pynfdump.Dumper('/data2/datasource/',profile='16/',sources=['nfcapd.201711161000','nfcapd.201711161005'])
    d = pynfdump.Dumper()
    ponce = 0
#    d.set_where(start=None,end=None,filename='/data2/datasource/16/nfcapd.201711161000')
    dstring = '/data2/datasource/16/'
    for i in range(6):
        nstr = str(i*5)
        if len(nstr) < 2:
            nstr = '0' + nstr
        dstring += 'nfcapd.2017111610' + nstr + ':'

    dstring = dstring[:-1]

    d.set_where(start=None,end=None,dirfiles='/data2/datasource/16/nfcapd.201711161030:nfcapd.201711161100')
#    d.set_where(start=None,end=None,filename='/data2/datasource/16/nfcapd.201711161000')

    records = d.search('proto icmp and host 166.111.8.241')

    fin = open('/data2/datasource/ICMP/time/10:00:00.txt','r')
    fout = open('./out2.txt','w')
    ip_dict = {}
    agg_dict = {}
    result_list = [0,0,0,0,0,0,0,0,0,0]

    for line in fin.readlines():
        items = line.split('#')
        ip = items[0].strip()
        string = items[1].split(',')
        if ip not in ip_dict:
            ip_dict[ip] = {'IN':[],'OUT':[],'ICMP':[string[1],string[2],string[3].strip()]}
            agg_dict[ip]= {\
                    'IN':[],\
                    'OUT':[],\
                    'ICMP':[string[1],string[2],string[3].strip()],\
                    'time_itv':-3,\
                    'flow_time_error_in':[0,0],\
                    'flow_time_error_out':[0,0],\
                    'first_time_error':False,\
                    'last_time_error':False }
            # -3: initialized but not assigned; -2 host unreachable -1: first_time_error(first time out is later than in) and last_time_error

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
        real_flow_dict['IN'] = [Flow.FlowGroup(srcip,srcport,dstip,dstport)]
        real_flow_dict['OUT'] = [Flow.FlowGroup(srcip,srcport,dstip,dstport)]
        i = 0
        for j in range(lgout):
            agg_dict[key]['flow_time_error_out'][1] += 1
            if ip_dict[key]['OUT'][j].get_first_time() > ip_dict[key]['OUT'][j].get_last_time():
                agg_dict[key]['flow_time_error_out'][0] += 1

            if real_flow_dict['OUT'][i].add_flow(ip_dict[key]['OUT'][j]):
                pass
            else:
                i += 1
                real_flow_dict['OUT'].append(Flow.FlowGroup(srcip,srcport,dstip,dstport))
                real_flow_dict['OUT'][i].add_flow(ip_dict[key]['OUT'][j])
#            except TypeError:
#                print key,ip_dict[key]
#                print j,ip_dict[key]['OUT'][j]
#                exit()

#        for item in ip_dict[key]:
#            print item.print_string()
#        print
#        for item in real_flow_dict[key]:
#            print item.display_string():
#        exit()

        i = 0
        for j in range(lgin):
            agg_dict[key]['flow_time_error_in'][1] += 1
            if ip_dict[key]['IN'][j].get_first_time() > ip_dict[key]['IN'][j].get_last_time():
                agg_dict[key]['flow_time_error_in'][0] += 1
                
            if real_flow_dict['IN'][i].add_flow(ip_dict[key]['IN'][j]):
                pass
            else:
                i += 1
                real_flow_dict['IN'].append(Flow.FlowGroup(srcip,srcport,dstip,dstport))
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
                if item['OUT'].get_first_time() > datetime.datetime(2017,11,16,10,58,0):
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

            

    for key in ip_dict:
        str1 = ' '.join(ip_dict[key]['ICMP'])
        prtstr = ''
        if agg_dict[key]['time_itv'] in [-1,-2,-3,-4]:
            t_itv = str(agg_dict[key]['time_itv'])
        else:
            t_itv = str(agg_dict[key]['time_itv'].seconds)+'s'+str(agg_dict[key]['time_itv'].microseconds)[:-3]+'ms'
        prtstr += \
            key+': '+str1+' '+\
            t_itv+\
            ' flowTimeErrorIn: '+str(agg_dict[key]['flow_time_error_in'][0])+':'+str(agg_dict[key]['flow_time_error_in'][1])+\
            ' flowTimeErrorOut: ' +str(agg_dict[key]['flow_time_error_out'][0])+':'+str(agg_dict[key]['flow_time_error_out'][1])+\
            ' FirstTimeError:'
        ddr = agg_dict[key]['flow_time_error_in'][0] + agg_dict[key]['flow_time_error_out'][0]
        if agg_dict[key]['first_time_error']:
            prtstr += ' True'
        else:
            prtstr += ' False'

        prtstr += ' LastTimeError:'
        if agg_dict[key]['last_time_error']:
            prtstr += ' True'
        else:
            prtstr += ' False'

        avi = [False,False]
        
        if float(agg_dict[key]['ICMP'][0]) == 10000:
            result_list[1] += 1

        if agg_dict[key]['time_itv'] == -2:
            result_list[0] += 1
            avi[0] = True
        elif agg_dict[key]['time_itv'] == -1:
            result_list[5] += 1
        elif agg_dict[key]['time_itv'] == -3:
            pass
        elif agg_dict[key]['time_itv'] == -4:
            pass
        elif agg_dict[key]['first_time_error']:
            result_list[8] += 1
            ivv = agg_dict[key]['time_itv']
            rtt = ivv.seconds*1000 + ivv.microseconds/1000
            rttmin = float(agg_dict[key]['ICMP'][1])
            rttmax = float(agg_dict[key]['ICMP'][2])
            rttavg = float(agg_dict[key]['ICMP'][0])
            if rttmin - 1 < rtt < rttmax + 1:
                result_list[6] += 1
                avi[0] = True
            if abs(rtt - rttavg)/rttavg < 0.1 or rttavg -2 < rtt < rttavg + 2:
                result_list[7] += 1
                avi[1] = True

        else:
            result_list[4] += 1
            ivv = agg_dict[key]['time_itv']
            rtt = ivv.seconds*1000 + ivv.microseconds/1000
            rttmin = float(agg_dict[key]['ICMP'][1])
            rttmax = float(agg_dict[key]['ICMP'][2])
            rttavg = float(agg_dict[key]['ICMP'][0])
            if rttmin - 1 < rtt < rttmax + 1:
                result_list[2] += 1
                avi[0] = True
            if abs(rtt - rttavg)/rttavg < 0.1 or rttavg -2 < rtt < rttavg + 2:
                result_list[3] += 1
                avi[1] = True

            if (not avi[0]) and (not avi[1]):
                print agg_dict[key]['time_itv']
                print t_itv
                print rtt,rttmin,rttmax,rttavg
#        if ponce < 10:
#            print agg_dict[key]['time_itv']
#            print t_itv
#            print rtt,rttmin,rttmax,rttavg
#            ponce += 1
        
        prtstr += ' '+str(avi[0])+' '+str(avi[1])

        prtstr += '\n\tIN:\n'

        fout.write(prtstr)
        for item in ip_dict[key]['IN']:
            fout.write('\t\t'+item.display_string()+'\n')
        fout.write('\n\tOUT:\n')
        for item in ip_dict[key]['OUT']:
            fout.write('\t\t'+item.display_string()+'\n')

        fout.write('\n')

        fout.write('\tAGG_IN\n')
        for item in agg_dict[key]['IN']:
            fout.write('\t\t'+item.display_string()+'\n')
        fout.write('\n\tAGG_OUT:\n')
        for item in agg_dict[key]['OUT']:
            fout.write('\t\t'+item.display_string()+'\n')

        fout.write('\n')


    print result_list

if __name__ == '__main__':
    main()
