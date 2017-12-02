#filename:Flow.py

import datetime
micro2m = 1000
#tsinghua_prefix = ('166.111','59.66','101.6','101.5','183.172','183.173','118.229')

class OneFlow():
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes):
        self.srcip = srcip
        self.dstip = dstip
        self.srcport = srcport
        self.dstport = dstport
        self.packets = packets
        self.tbytes = tbytes
        self.first = first + datetime.timedelta(microseconds = msec_first*micro2m)
        self.last = last + datetime.timedelta(microseconds = msec_last*micro2m)

    def time2str(self):
        first_time = self.first.strftime('%H:%M:%S:%f')[:-3]
        last_time = self.last.strftime('%H:%M:%S:%f')[:-3]
        return (first_time,last_time)

class IcmpFlow(OneFlow):
    def __init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes,flowtype):
        OneFlow.__init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes)
        if self.packets == 0:
            raise ValueError('icmp type can not be analyzed!')
        else:
            if flowtype == 'OUT':
                afix = 'OUT'
            elif flowtype == 'IN':
                afix = 'IN'
            else:
                raise ValueError('Flow type error')

            if self.dstport == 2048:
                self.ftype = 'REQUEST_'
            elif 768 < self.dstport < 782:
                self.ftype = 'UNREACHABLE_'
            elif self.dstport in [2816, 2817]:
                self.ftype = 'TIMEOUT_'
            elif self.dstport == 0:
                self.ftype = 'RESPONSE_'
            else:
                self.ftype = 'OTHER_'

            self.ftype = self.ftype + afix

    def display_list(self):
        return [self.srcip, self.srcport, self.dstip, self.dstport, self.first, self.last, self.packets, self.tbytes, self.ftype]

    def display_dict(self):
        res = {'srcip':self.srcip, 'srcport':self.srcport, 'dstip':self.dstip, 'dstport':self.dstport, 'first':self.first ,'last':self.last, 'packets':self.packets, 'bytes':self.tbytes, 'type':self.ftype}
        return res

    def display_string(self):
        s =  [self.srcip+':'+str(self.srcport), self.dstip+':'+str(self.dstport), self.time2str()[0], self.time2str()[1], str(self.packets), str(self.tbytes), self.ftype]
        return ' '.join(s)


    def get_first_time(self):
        return self.first_time

    def get_last_time(self):
        return self.last_time

    def get_pnumber(self):
        return self.packets, self.tbytes

    def flowtype(self):
        return self.flow_type

    def get_srcip(self):
        return self.srcip
    
    def get_dstip(self):
        return self.dstip

    def cal_delta(oth):
        return oth.get_first_time() - self.first_time
        
