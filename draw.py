import pandas as pd
import datetime
import os
import numpy as np
import shutil
import matplotlib.pyplot as plt


# out_head = ['tripduration', 'starttime', 'stoptime', 'start station id',
#             'start station name', 'start station latitude',
#             'start station longitude', 'end station id', 'end station name',
#             'end station latitude', 'end station longitude', 'bikeid', 'usertype',
#             'birth year', 'gender']

def count():
    save_dir = "/data/DiskData/bike_3-5/bike_image"
    zero_time = datetime.date(year=2021, month=3, day=1)
    try:
        shutil.rmtree(save_dir)
    except Exception:
        pass

    df = pd.read_csv("/data/DiskData/bike_3-5/jrj_like_NYC.csv", parse_dates=['starttime', 'stoptime'])
    df_head = ['in_flow', 'out_flow']
    df_index = list(range(0, 24))
    stations = dict()
    data = dict()
    for index, row in df.iterrows():
        start_station_id = row['start station id']
        end_station_id = row['end station id']
        if start_station_id not in stations:
            stations[start_station_id] = row['start station name']
            data[start_station_id] = np.zeros(shape=(122, 24, 2))
        opposite_day = row['starttime'] - zero_time
        data[start_station_id][int(opposite_day.days)][int(row['starttime'].seconds / 3600)][0] += 1

        if end_station_id not in stations:
            stations[end_station_id] = row['start station name']
            data[end_station_id] = np.zeros(shape=(122, 24, 2))
        opposite_day = row['endtime'] - zero_time
        data[start_station_id][int(opposite_day.days)][int(row['stoptime'].seconds / 3600)][1] += 1
    for station in stations:
        if not os.path.exists(os.path.join(save_dir, station)):
            os.makedirs(os.path.join(save_dir, station))


if __name__ == '__main__':
    count()
    # for m in range(12):
    #     if not os.path.exists('/Users/mulan/Documents/bike_image/month%d' % (m + 1)):
    #         os.makedirs('/Users/mulan/Documents/bike_image/month%d' % (m + 1))
    #     for d in range(31):
    #         df = pd.read_csv('/Users/mulan/Documents/bike_data/month%d/day%d.csv' % (m + 1, d + 1))
    #         in_flow = df['in_flow'].values.tolist()
    #         out_flow = df['out_flow'].values.tolist()
    #
    #         fig = plt.figure()
    #         ax = plt.axes()
    #
    #         plt.plot(range(0, 24), in_flow, color='b', marker='+', label='in')
    #         plt.plot(range(0, 24), out_flow, color='g', marker='+', label='out')
    #
    #         plt.legend()
    #         plt.savefig('/Users/mulan/Documents/bike_image/month%d/day%d.png' % (m + 1, d + 1))
    #         plt.close()
