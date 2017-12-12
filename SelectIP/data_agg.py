#!/bin/python
import datetime
import json
import Flow


def main():
    ip_dict = {}
    fout = open('stat.txt','w')
    for i in range(23):
        if i < 10:
            continue
        hour = str(i)
        for j in range(2):
            if j == 0:
                tkey = hour + ':00:00'
                dpath = 'fout/out.'+hour+':00:00.json'

            if j == 1:
                tkey = hour + ':30:00'
                dpath = 'fout/out.'+hour+':30:00.json'

            fin = open(dpath,'r')
            line = fin.readline().strip()
            temp_dict = json.loads(line)
            for ip in temp_dict:
                if ip not in ip_dict:
                    ip_dict[ip] = {tkey:temp_dict[ip]}
                else:
                    ip_dict[ip][tkey] = temp_dict[ip]
    
    for key in ip_dict:
        first_metrics = [0,0]
        second_metrics = [0,0]
        for mkey in ip_dict[key]:
            first_metrics[1] += 1
            second_metrics[1] += 1
            if ip_dict[key][mkey]['result1']:
                first_metrics[0] += 1
            if ip_dict[key][mkey]['result2']:
                second_metrics[0] += 1
        print key,first_metrics,second_metrics

if __name__ == '__main__':
    main()
