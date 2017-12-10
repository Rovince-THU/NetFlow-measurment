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
        if msec_first != 0:
            self.first_time = first + datetime.timedelta(microseconds = msec_first*micro2m)
        else:
            self.first_time = first
        if msec_last != 0:
            self.last_time = last + datetime.timedelta(microseconds = msec_last*micro2m)
        else:
            self.last_time = last

    def time2str(self):
        first = self.first_time.strftime('%H:%M:%S:%f')[:-3]
        last = self.last_time.strftime('%H:%M:%S:%f')[:-3]
        return (first,last)

    def get_first_time(self):
        return self.first_time

    def get_last_time(self):
        return self.last_time

    def get_pnumber(self):
        return self.packets, self.tbytes

    def get_srcip(self):
        return self.srcip, self.srcport
    
    def get_dstip(self):
        return self.dstip, self.dstport

    def cal_delta(self,oth):
        return oth.get_first_time() - self.first_time

    def is_first_early(self,oth):
        if self.first_time < oth.get_first_time():
            return True
        else:
            return False

    def is_first_later(self,oth):
        if self.first_time > oth.get_first_time():
            return True
        else:
            return False

    def is_last_early(self,oth):
        if self.last_time < oth.get_last_time():
            return True
        else:
            return False

    def is_last_later(self,oth):
        if self.last_time > oth.get_last_time():
            return True
        else:
            return False

    def print_string(self):
        prtstr = self.srcip + ' => ' + self.dstip + ':' + str(self.dstport) + ' '+ self.first_time.strftime("%Y-%m-%d %H:%M:%S.%f") + ' ' + self.last_time.strftime("%Y-%m-%d %H:%M:%S.%f")+ ' ' + str(self.packets) + ' ' + str(self.tbytes)
        return prtstr

class FlowGroup(OneFlow):
    def __init__(self,srcip,srcport,dstip,dstport,first = datetime.datetime(2000,1,1,0,0,0),msec_first = 0,last = datetime.datetime(2000,1,1,0,0,0),msec_last = 0,packets = 0,tbytes = 0,flowcount = 0):
        OneFlow.__init__(self,srcip,srcport,dstip,dstport,first,msec_first,last,msec_last,packets,tbytes)
        self.flowcount = flowcount

    def set_first_time(self,time):
        self.first_time = time

    def set_last_time(self,time):
        self.last_time = time

    def set_flowcount(self,count):
        self.flowcount = count

    def set_tuple(self,srcip,srcport,dstip,dstport):
        self.srcip = srcip
        self.srcport = srcport
        self.dstip = dstip
        self.dstport = dstport

    def add_flow(self,flow):
        if self.flowcount == 0:
            self.first_time = flow.get_first_time()
            self.last_time = flow.get_last_time()
            self.packets, self.tbytes = flow.get_pnumber()
            self.flowcount += 1
            return True
        if flow.get_first_time() - self.first_time > datetime.timedelta(0,10,0):
            return False
        else:
            self.flowcount += 1
            self.packets += flow.get_pnumber()[0]
            self.tbytes += flow.get_pnumber()[1]
            if flow.get_first_time() < self.get_first_time():
                self.first_time = flow.get_first_time()
            if flow.get_last_time() > self.get_last_time():
                if flow.get_first_time() - self.first_time > datetime.timedelta(0,2,0) and flow.get_first_time() > self.last_time:
                    return True
                else:
                    self.last_time = flow.get_last_time()
                    return True
            else:
                return True

    def display_string(self):
        return self.print_string() + ' ' + str(self.flowcount) 

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
        return [self.srcip, self.srcport, self.dstip, self.dstport, self.first_time, self.last_time, self.packets, self.tbytes, self.ftype]

    def display_dict(self):
        res = {'srcip':self.srcip, 'srcport':self.srcport, 'dstip':self.dstip, 'dstport':self.dstport, 'first':self.first_time ,'last':self.last_time, 'packets':self.packets, 'bytes':self.tbytes, 'type':self.ftype}
        return res

    def display_string(self):
        s =  [self.srcip+':'+str(self.srcport), self.dstip+':'+str(self.dstport), self.time2str()[0], self.time2str()[1], str(self.packets), str(self.tbytes), self.ftype]
        return ' '.join(s)

    def flowtype(self):
        return self.flow_type


        
