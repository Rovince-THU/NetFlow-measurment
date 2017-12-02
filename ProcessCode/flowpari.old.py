#filename:FlowPair.py

from Flow import IcmpFlow, DnsFlow, Tcp80Flow #Here is annoted orginally

#ip_pair,port_pair,flow_dict-a 2 keys dictionary
class OneFlowPair():
    def __init__(self,oneflow):
        self.ip_pair = oneflow.ipkey
        self.port_pair = oneflow.portkey
        self.flow_dict = {'flow1':[],'flow2':[]}
        self.flow_dict[oneflow.flowlabel].append(oneflow)

    def addflow(self,oneflow):
        self.flow_dict[oneflow.flowlabel].append(oneflow)

    def get_length(self):
        length_list = []
        for flow in self.flow_dict:
            length_list.append(len(self.flow_dict[flow]))
        return length_list

    def sort(self):
        for flow in self.flow_dict:
            self.flow_dict[flow] = sorted(self.flow_dict[flow], key=lambda f: f.first)

    def filter(self):
        fcount = 0
        self.sort()
        length = min(self.get_length())
        for flow in self.flow_dict:
            if len(self.flow_dict[flow])>length:
                fcount += len(self.flow_dict[flow]) - length
                self.flow_dict[flow] = self.flow_dict[flow][:length]
        return fcount

    def display(self):
        length = min(self.get_length())
        str1 = str2 = ''
        sep = ('-'*150)+'\n'
        flag = True
        for i in range(length):
            temp_str1 = self.flow_dict['flow1'][i].display()
            temp_str2 = self.flow_dict['flow2'][i].display()
            str_len = max(len(temp_str1),len(temp_str2))+5
            str1 += (temp_str1+' '*(str_len-len(temp_str1)))
            str2 += (temp_str2+' '*(str_len-len(temp_str2)))
            if i%3==2:
                print (str1+'\n'+str2) if flag else (sep+str1+'\n'+str2)
                flag = False
                str1 = str2 = ''
        if i%3!=2:
            print (str1+'\n'+str2) if flag else (sep+str1+'\n'+str2)

class IcmpFlowPair(OneFlowPair):
    def filter(self):
        fcount = OneFlowPair.filter(self) 
        length = min(self.get_length())
        del_list = []
        for i in range(length):
            if self.flow_dict['flow1'][i].packets != self.flow_dict['flow2'][i].packets:
                del_list.append(i)
        del_list = sorted(del_list,reverse=True)
        for i in del_list:
            del self.flow_dict['flow1'][i]
            del self.flow_dict['flow2'][i]
            fcount += 2
        return fcount
    
    def display(self):
        OneFlowPair.display(self)

class DnsFlowPair(OneFlowPair):
    def filter(self):
        fcount = OneFlowPair.filter(self)
        #self.sort()
        return fcount

    def display(self):
        print self.port_pair
        OneFlowPair.display(self)

class TcpFlowPair(OneFlowPair):
    def filter(self):
        fcount = OneFlowPair.filter(self)
        #self.sort()
        return fcount

    def display(self):
        print self.port_pair
        OneFlowPair.display(self)
