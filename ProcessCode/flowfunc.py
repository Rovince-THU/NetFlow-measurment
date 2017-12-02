#filename:flowfunc.py

#from FlowModule.Flow import IcmpFlow, DnsFlow, TcpFlow
#from FlowModule.IpPair import IcmpIpPair, DnsIpPair, TcpIpPair
import pynfdump
import datetime

DataPath = '../'
Nfprofile = 'data/netflow_data'
Nfsources = '201509/0708/'
SearchTypeDict = {'icmp':'proto icmp','dns':'port 53 and proto udp','tcp':'proto tcp'}
TimeBegin = datetime.datetime(2015,9,7,12,0,0)
TimeEnd = datetime.datetime(2015,9,9,0,0,0)

#The function is to match the IP data flow in to IP pais 
def data_aggregation(datatype,records):
    if datatype == 'icmp':
        from FlowModule.Flow import IcmpFlow as AFlow
        from FlowModule.IpPair import IcmpIpPair as AIpPair
    elif datatype == 'dns':
        from FlowModule.Flow import DnsFlow as AFlow
        from FlowModule.IpPair import DnsIpPair as AIpPair
    elif datatype == 'tcp':
        from FlowModule.Flow import TcpFlow as AFlow
        from FlowModule.IpPair import TcpIpPair as AIpPair
    else:
        raise TypeError('data type is wrong!')
    fcount = 0 #fcount is the number of flows in records(flowcount)
    pcount = 0 #pcount is the number of pairs in records(paircount)
    ippair_dict={}
    for r in records:
        try:
            aflow = AFlow(srcip=r['srcip'],srcport=r['srcport'],dstip=r['dstip'],dstport=r['dstport'],
                          first=r['first'],msec_first=r['msec_first'],last=r['last'],msec_last=r['msec_last'],
                          packets=r['packets'],tbytes=r['bytes'])
            fcount += 1
            if not aflow.ipkey in ippair_dict:
                pcount += 1
                ippair_dict[aflow.ipkey] = AIpPair(aflow)
            else:
                ippair_dict[aflow.ipkey].addflow(aflow)
        except ValueError:
            pass
    #Here, ippair_dict is a dictionary with key(ip-tsinghua,ip-outside), value: a flow list
    #Following is to filter and deletion
    del_list = []
    for key in ippair_dict:
        fcount -= ippair_dict[key].filter()
        if ippair_dict[key].ifdel():
            del_list.append(key)
            pcount -= 1
    for key in del_list:
        del ippair_dict[key]

    return ippair_dict

def flow2str(ippair_dict,datatype):
    for ipkey in ippair_dict:
        srcip,dstip = ipkey
        for portkey in ippair_dict[ipkey].flowpair_dict:
            srcport,dstport = portkey
            if datatype == 'icmp':
                str1 = '{0},{1},'.format(srcip,dstip)
            else:
                str1 = '{0},{1},{2},{3},'.format(srcip,dstip,srcport,dstport)
            temp1 = ippair_dict[ipkey].flowpair_dict[portkey].flow_dict['flow1']
            temp2 = ippair_dict[ipkey].flowpair_dict[portkey].flow_dict['flow2']
            length = ippair_dict[ipkey].flowpair_dict[portkey].get_length()
            for i in range(min(length)):
                f1,l1 = temp1[i].time2str()
                f2,l2 = temp2[i].time2str()
                str2 = "{0},{1},{2},{3},".format(f1,l1,temp1[i].packets,temp1[i].bytes)
                str3 = "{0},{1},{2},{3},".format(f2,l2,temp2[i].packets,temp2[i].bytes)
                fstr = str1+str2+str3
                yield dstip,fstr

#In This function we use a IP_list and datatye(e.g. ICMP UDP), and write the IP pairs to a csv. Each ip has a unique file.
#Futhermore, IP pairs is matched in function data_aggregation
def search(ip_list,datatype):
    str1 = ""
    for ip in ip_list:
        str1 += (" host "+str(ip)+" or")
    str1 = str1[:-2]
    str2 = " and({0})".format(str1)
    query = SearchTypeDict[datatype] + str2
    timedelta = datetime.timedelta(minutes=30)
    timeflag = TimeBegin
    output_dict = {}
    for ip in ip_list:
        output_dict[ip] = open('../data/measurement_data/icmpresult/{addr}.csv'.format(addr=ip),'w')
    while 1: #This loop is to read the files one after another to fetch the result, aggregation is done for every read
        dirname = timeflag.strftime('%Y%m%d%H%M')
        timeflag += timedelta
        if timeflag>TimeEnd:break
        d = pynfdump.Dumper(DataPath,profile=Nfprofile,sources=[Nfsources+dirname])
        print dirname
        for key in output_dict:
            output_dict[key].write("##{0}\n".format(dirname))
        d.set_where(start=None, end=None)
        records = d.search(query)
        ippair_dict = data_aggregation(datatype,records)
        fstr_all = flow2str(ippair_dict,datatype)
        for ftuple in fstr_all:
            output_dict[str(ftuple[0])].write(ftuple[1]+"\n")
    for key in output_dict:
        output_dict[key].close()
 
