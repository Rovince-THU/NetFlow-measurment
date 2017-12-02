#filename:Flow.py

import datetime
micro2m = 1000
tsinghua_prefix = ('166.111','59.66','101.6','101.5','183.172','183.173','118.229')

class OneFlow():
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes):
        self.srcip = srcip
        self.dstip = dstip
        self.srcport = srcport
        self.dstport = dstport
        self.ipkey,self.portkey = self.affirm_key()#(Tsinghua IP,Outside IP),(Tsinghua Port, OutSide Port)
        self.packets = packets
        self.bytes = tbytes
        self.first = first + datetime.timedelta(microseconds = msec_first*micro2m)
        self.last = last + datetime.timedelta(microseconds = msec_last*micro2m)

    def time2str(self):
        first_time = self.first.strftime('%H:%M:%S:%f')
        last_time = self.last.strftime('%H:%M:%S:%f')
        return(first_time,last_time)

    def display(self):
        first_time,last_time = self.time2str()
        res = first_time + '=>' + last_time + '|'+str(self.srcport)+'->'+str(self.dstport)+'|'+str(self.packets)+','+str(self.bytes)+'|'
        return res 

    def affirm_key(self):
        ip_list = sorted([(self.srcip,self.srcport),(self.dstip,self.dstport)],key=lambda f: f[0])
        for i in range(2):
            for prefix in tsinghua_prefix:
                if str(ip_list[i][0]).startswith(prefix):
                    return (ip_list[i][0],ip_list[(i+1)%2][0]),(ip_list[i][1],ip_list[(i+1)%2][1])
        else:
            raise ValueError('no tsinghua ip prefix!')

class IcmpFlow(OneFlow):
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes):
        OneFlow.__init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes)
        if self.packets == 0:
            raise ValueError('icmp type can not be analyzed!')
        else:
            if self.srcip == self.ipkey[0] and self.dstport == 2048:
                self.flowlabel = 'flow1'
            elif self.srcip == self.ipkey[1] and self.dstport == 0:
                self.flowlabel = 'flow2'
            else:
                raise ValueError('icmp type can not be analyzed!')

    def display(self):
        first_time,last_time = self.time2str()
        sep = '|' if self.dstport == 2048 else '   |'
        res = first_time + '=>' + last_time + '|'+str(self.srcport)+'->'+str(self.dstport)+sep+str(self.packets)+','+str(self.bytes)+'|'
        return res

class DnsFlow(OneFlow):
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes):
        OneFlow.__init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes)
        if self.srcip == self.ipkey[0] and self.dstport == 53:
            self.flowlabel = 'flow1'
        elif self.srcip == self.ipkey[1] and self.srcport == 53:
            self.flowlabel = 'flow2'
        else:
            raise ValueError('dns type can not be analyzed!')

class Tcp80Flow(OneFlow):
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes):
        OneFlow.__init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes)
        if self.srcip == self.ipkey[0] and self.dstport == 80:
            self.flowlabel = 'flow1'
        elif self.srcip == self.ipkey[1] and self.srcport == 80:
            self.flowlabel = 'flow2'
        else:
            raise ValueError('dns type can not be analyzed!')

class TcpFlow(OneFlow):
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes):
        OneFlow.__init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes)
        if self.srcip == self.ipkey[0]:
            self.flowlabel = 'flow1'
        elif self.srcip == self.ipkey[1]:
            self.flowlabel = 'flow2'
        else:
            raise ValueError('dns type can not be analyzed!')
