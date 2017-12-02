#!/bin/python
#This file is the main function of the program

#Here we extract the IP list from the HH:MM:SS.txt

import sys,os
import logging
from ProcessCode import process


def main():
#    InnerIP_list = ['166.111','59.66','101.5','101.6','183.172','183.173','118.229']
    ip_list = []
    fin = open('/data2/datasource/ICMP/time/10:00:00.txt','r')
    line = fin.readline()
    while line:
        IP = line.split('#')[0]
        if IP not in ip_list:
            ip_list.append(IP)
        try:
            line = fin.readline()
        except ValueError:
            logging.debug('file not end normally')
            break


    process.search(ip_list)

    



if __name__ == '__main__':
    main()


