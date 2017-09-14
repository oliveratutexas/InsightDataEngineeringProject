import datetime
import tinys3
# import wget
import os
import subprocess
import urllib3
import urllib
import sys
from collections import namedtuple


comments_base_url = 'http://files.pushshift.io/reddit/comments/'

MonthYearPair = namedtuple('MonthYearPair','month year')

start_tuple = MonthYearPair(12,5)
#If you're trying to incorporate this into your project, be sure to change this!
# end_tuple = (07,17)
end_tuple = MonthYearPair(1,7)

def inc_date_tup(date_tup):
    mo = -1
    if date_tup[0] == 12:
        mo = 1
        yr = date_tup[1] + 1
    else:
        mo = date_tup[0] + 1
        yr = date_tup[1]
    print((mo,yr))

    return (mo,yr)

def next_date_gen(start_tuple,end_tuple):
    cur_tuple = start_tuple
    while(cur_tuple != end_tuple):
        yield cur_tuple
        cur_tuple = inc_date_tup(cur_tuple)


def start_upload(bucket_name):

    if(len(sys.argv) < 3):
        print('You need your ACCESS_KEY and SECRET key from amazon as arguments.')

    ACCESS_KEY = sys.argv[1]
    SECRET_KEY = sys.argv[2]

    s3_handle = tinys3.Connection(ACCESS_KEY,SECRET_KEY,endpoint='s3-us-west-2.amazonaws.com')

    for date_tup in next_date_gen(start_tuple,end_tuple):
        file_name = 'RC_20{1:02d}-{0:02d}.bz2'.format(*date_tup)
        stripped_name = file_name[:-4]
        cur_url = 'https://files.pushshift.io/reddit/comments/{}'.format(file_name)

        print(cur_url)

        #TODO - add file check
        #Download file
        # urllib.robotparser.
        # urllib.request.urlretrieve(cur_url,filename=file_name,)
        subprocess.call('wget {}'.format(cur_url).split())

        #unzip the file
        subprocess.call('bzip2 -d {}'.format(file_name).split())

        #upload the file
        with open(stripped_name,'rb') as fh:
            #Upload file
            # s3_handle.upload('{}.json'.format(stripped_name),fh,bucket_name)
            subprocess.call('aws s3 cp {0} s3://{1}'.format(stripped_name,bucket_name).split())

        #Delete the file to save space
        subprocess.call('rm {}'.format(file_name).split())
        subprocess.call('rm {}'.format(stripped_name).split())

start_upload('heyyall')


