import pandas as pd
import datetime


def reconstruct():
    path = "/data/DiskData/bike_3-5/final_noRep.csv"
    out_path = "/data/DiskData/bike_3-5/jrj_order.csv"
    out_head = ['tripduration', 'starttime', 'stoptime', 'start station id',
                'start station name', 'start station latitude',
                'start station longitude', 'end station id', 'end station name',
                'end station latitude', 'end station longitude', 'bikeid', 'usertype',
                'birth year', 'gender']
    df = pd.read_csv(path, parse_dates=['STATS_TIME', 'RECV_TIME'])
    tripduration, starttime, stoptime, start_station_id, \
    start_station_name, start_station_latitude, start_station_longitude, end_station_id, \
    end_station_name, end_station_latitude, end_station_longitude, bikeid, \
    usertype, birth_year, gender = \
        [], [], [], [], \
        [], [], [], [], \
        [], [], [], [], \
        [], [], []
    bike_id, station = '', ''
    pre_recv_time = datetime.timedelta()
    bad_num = 0
    good_num = 0
    for index, row in df.iterrows():
        if row['IDENTITY_NO'] != bike_id:
            bike_id = row['IDENTITY_NO']
            pre_recv_time = row['RECV_TIME']
            continue
        if row['MONITOR_ID'] == station:
            print("Warning: redundant found in index %d" % index)
            pre_recv_time = row['RECV_TIME']
            continue
        duration = row['STATS_TIME'] - pre_recv_time
        if duration.days > 0 or duration.seconds > 1800:
            bad_num += 1
            pre_recv_time = row['RECV_TIME']
            continue
        pre_recv_time = row['RECV_TIME']
        good_num += 1
        if index % 10000 == 0:
            print("%d/%d" % (bad_num, good_num))


def count():
    path = "/data/DiskData/nyc-citibike-data-master/data/202001-citibike-tripdata.csv"
    df = pd.read_csv(path)
    print(df.iloc[0]["starttime"])


if __name__ == '__main__':
    reconstruct()
