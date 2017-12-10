import pynfdump
import datetime
import Flow


def main():
    #d = pynfdump.Dumper('/data2/datasource/',profile='16/',sources=['nfcapd.201711161000','nfcapd.201711161005'])
    d = pynfdump.Dumper()
#    d.set_where(start=None,end=None,filename='/data2/datasource/16/nfcapd.201711161000')
    dstring = '/data2/datasource/16/'
    for i in range(6):
        nstr = str(i*5)
        if len(nstr) < 2:
            nstr = '0' + nstr
        dstring += 'nfcapd.2017111610' + nstr + ':'

    dstring = dstring[:-1]

    d.set_where(start=None,end=None,dirfiles='/data2/datasource/16/nfcapd.201711161000:nfcapd.201711161030')
#    d.set_where(start=None,end=None,filename='/data2/datasource/16/nfcapd.201711161000')

    records = d.search('proto icmp and host 166.111.8.241')
    timedict = {-1:0}

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
        first_time = first + datetime.timedelta(microseconds = msec_first)
        last_time = last + datetime.timedelta(microseconds = msec_last)

        tt = last_time - first_time
        if tt < datetime.timedelta(0,0,0):
            timedict[-1] += 1
            continue

        ts = int(tt.total_seconds())
        if ts not in timedict:
            timedict[ts] = 1
        else:
            timedict[ts] += 1
        
    for key in timedict:
        print str(key)+'\t'+str(timedict[key])

if __name__ == '__main__':
    main()
