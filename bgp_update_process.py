import signal, time, datetime, json, wget, urllib, math, calendar, os, sys, inspect
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from extract_feature import FeatureExtractor

'''
    Design:
    1. fetch data: get data from specified rrc & time period using bgp broker, 
        - input: data start & end
        - create directory for each;
    2. process : call FeatureExtractor to generate a matrix, & write into csv
        - input: anomaly start & end
        - also note to count the rows
        - note the labeling
'''

def fetch_data(d_start, d_end, rrc, project='ris'):
    # get all the download links
    links = []
    for ti in range(d_start, d_end, 3600):
        query_url = 'https://bgpstream.caida.org/broker/data?intervals[]=%s,%s&collectors[]=%s&collectors[]=%s&types[]=updates' % (ti, ti + 3600, project, rrc)
        session = Session()
        try:
            response = session.get(query_url)
            text = json.loads(response.text)
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            print(e)
            return 0
        tmp = [r['url'] for r in text['data']['dumpFiles']]
        print(tmp)
        links += tmp
    print(links)

    # make directory and downlaod update 
    folder = 'updates_%s_%s-%s' % (rrc, d_start, d_end)
    paths = []
    try: os.mkdir(folder, 0o777)
    except OSError: print('Directory cannot be created (maybe existed)')
    nameList = []
    loss = 0
    for url in links:
        pos = url.find('update')
        filename = url[pos:]
        path = os.path.join(folder, filename)
        paths += [path]
        # print(url, '\n', filename, '\n', path)
        try:
            absPath = wget.download(url=url, out=path)
            nameList += [absPath]
            # paths += [path]

        except:
            loss += 1
            pass
    print('\n', len(nameList), 'files downloaded! loss: ', loss)
    return folder, paths
    
def parse_data(folder, paths, a_start, a_end):
    # parse the updates file and generate ARFF file
    extractor = FeatureExtractor(paths, 'extr-' + folder)
    extractor.header()
    extractor.process(a_start, a_end)

def main():
    pattern = '%Y-%m-%d-%H-%M-%S'
    rrc = sys.argv[1]
    d_start = int(calendar.timegm(time.strptime(sys.argv[-4], pattern)))
    d_end = int(calendar.timegm(time.strptime(sys.argv[-3], pattern)))
    a_start = int(calendar.timegm(time.strptime(sys.argv[-2], pattern))) // 60
    a_end = int(calendar.timegm(time.strptime(sys.argv[-1], pattern))) // 60

    folder, paths = fetch_data(d_start, d_end, rrc)
    for path in paths:
        # print(path)
        cmd = 'python extract_feature.py %s extr-%s %s %s' % (path, folder, sys.argv[-2], sys.argv[-1])
        try:
            os.system(cmd)
        except KeyboardInterrupt:
            x = input('Want to continue? (y/n)')
            if x == 'y': pass
            else: 
                print('Stopped.')
                return 0
            # print('path', path, 'failed...')
        
    # parse_data(folder, paths, a_start, a_end)

if __name__ == '__main__':
    main()