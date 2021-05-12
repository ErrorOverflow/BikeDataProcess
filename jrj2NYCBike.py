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
    data = [[] for _ in range(15)]
    # tripduration, starttime, stoptime, start_station_id, \
    # start_station_name, start_station_latitude, start_station_longitude, end_station_id, \
    # end_station_name, end_station_latitude, end_station_longitude, bikeid, \
    # usertype, birth_year, gender = \
    #     [], [], [], [], \
    #     [], [], [], [], \
    #     [], [], [], [], \
    #     [], [], []
    bike_id, pre_station = '', ''
    pre_recv_time = datetime.timedelta()
    for index, row in df.iterrows():
        if row['IDENTITY_NO'] != bike_id:
            bike_id = row['IDENTITY_NO']
            pre_station = row['MONITOR_ID']
            pre_recv_time = row['RECV_TIME']
            continue
        if row['MONITOR_ID'] == pre_station:
            # print("Warning: redundant found in index %d" % index)
            pre_station = row['MONITOR_ID']
            pre_recv_time = row['RECV_TIME']
            continue
        duration = row['STATS_TIME'] - pre_recv_time
        if duration.days > 0 or duration.seconds > 1800:
            pre_station = row['MONITOR_ID']
            pre_recv_time = row['RECV_TIME']
            continue
        data[0].append(duration.seconds)
        data[1].append(str(pre_recv_time))
        data[2].append(str(row['STATS_TIME']))
        data[3].append(pre_station)
        data[7].append(row['MONITOR_ID'])
        data[11].append(row['IDENTITY_NO'])
        if index % 10000 == 0:
            print("%d" % index)
    data[4] = ["1 Ave & E 16 St" for _ in range(len(data[0]))]
    data[5] = [0.0 for _ in range(len(data[0]))]
    data[6] = [0.0 for _ in range(len(data[0]))]
    data[8] = ["Canal St & Rutgers St" for _ in range(len(data[0]))]
    data[9] = [0.0 for _ in range(len(data[0]))]
    data[10] = [0.0 for _ in range(len(data[0]))]
    data[12] = ["Subscriber" for _ in range(len(data[0]))]
    data[13] = [1992 for _ in range(len(data[0]))]
    data[14] = [0 for _ in range(len(data[0]))]
    out_dic = dict(zip(out_head, data))
    out_data = pd.DataFrame(out_dic)
    print(out_data.head())
    out_data.to_csv("/data/DiskData/bike_3-5/jrj_like_NYC.csv", index=False)


def count():
    path = "/data/DiskData/nyc-citibike-data-master/data/202001-citibike-tripdata.csv"
    df = pd.read_csv(path)
    for key in df.columns:
        print(type(df.iloc[0][key]))


if __name__ == '__main__':
    reconstruct()
