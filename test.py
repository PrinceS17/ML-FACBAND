
import wget
import os
import math
import mrtparse
from mrtparse import *


''' 
    Given start and end data, download all the update messages from ripe.
    Basically, we want it to be the same year and same month.

    Test passed on 2019.3.6.
'''

def downloadFromUrl(yr, mon, start, end):

    # basic string definition & set the path
    baseUrl = 'http://data.ris.ripe.net/rrc04/'
    folder = '/home/jason/Downloads/CS538/Project/dataset/rrc04_' + yr + mon + start + '-' + end
    nameList = []

    try:  
        os.mkdir(folder, 0o777)
    except OSError:  
        print ("Creation of the directory %s failed, maybe existed..." % folder)

    # while loop to download all the data file we need
    print('Downloading files ... ')
    N = (int(end) - int(start) + 1) * 96
    i = 0
    loss = 0
    while i < N:
        min = repr( (i % 4) * 15 )
        if len(min) < 2:
            min = '0' + min
        hour = repr( math.floor(i / 4) % 24 )
        if len(hour) < 2:
            hour = '0' + hour
        day = repr( int(start) + math.floor(i / 96) % 30 )
        if len(day) < 2:
            day = '0' + day
        cur = yr + mon + day
        i += 1
        filename = 'updates.' + cur + '.' + hour + min + '.gz'
        # print("current file: ", filename)
        
        url = baseUrl + yr + '.' + mon + '/' + filename
        path = folder + '/' + filename
        try:
            absPath = wget.download(url, path)
            nameList.append(absPath)
        except:
            loss += 1
            pass
    print('\n', len(nameList), 'files downloaded.')
    return nameList

# test passed
flist = downloadFromUrl('2003', '01', '21', '21')
d = Reader(flist[0])
for m in d:
    
    print(m)
