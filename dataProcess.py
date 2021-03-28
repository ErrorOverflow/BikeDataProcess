import pandas as pd
import numpy as np
import datetime
import os
import matplotlib as plt
import multiprocessing as mp
import math


def MarsTransBMap(lng, lat):
    x_pi = math.pi * 3000.0 / 180.0
    x = lng
    y = lat
    z = math.sqrt(x * x + y * y) + 0.00002 * math.sin(y * x_pi)
    theta = math.atan2(y, x) + 0.000003 * math.cos(x * x_pi)
    new_lng = z * math.cos(theta) + 0.0065
    new_lat = z * math.sin(theta) + 0.006
    return new_lng, new_lat


def MarsStr2BMap(lng_str, lat_str):
    mars_lng = float(lng_str)
    mars_lat = float(lat_str)
    return MarsTransBMap(mars_lng, mars_lat)


def save_data_csv(in_flow, out_flow):
    in_data = pd.Series(in_flow)
    out_data = pd.Series(out_flow)
    in_data.to_csv('/Users/mulan/Documents/in_flow.csv', index=False, header=False)
    out_data.to_csv('/Users/mulan/Documents/out_flow.csv', index=False, header=False)


def jinrongjie2slim():
    size = 2e7
    reader = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv', chunksize=size)
    i = 0
    for chunk in reader:
        drop_list = []
        base = int(size * i)
        drop_num = len(chunk)
        pre_index = -1
        pre_id = ""
        pre_monitor = ""
        recv_time = ["" for _ in range(len(chunk))]
        for index, row in chunk.iterrows():
            if index == base + len(chunk) - 1:
                recv_time[pre_index - base] = chunk.iloc[index - 1 - base]['STATS_TIME']
                drop_list += list(range(pre_index + 1, index + 1))
                continue
            if row['IDENTITY_NO'] == pre_id and row['MONITOR_ID'] != pre_monitor and index > pre_index + 1:
                drop_list += list(range(pre_index + 1, index))
                recv_time[pre_index - base] = chunk.iloc[index - 1 - base]['STATS_TIME']
                drop_num -= 1
                pre_index = index
                pre_monitor = row['MONITOR_ID']
            if row['IDENTITY_NO'] != pre_id:
                if pre_index != -1:
                    drop_list += list(range(pre_index, index))
                pre_index = index
                pre_id = row['IDENTITY_NO']
                pre_monitor = row['MONITOR_ID']
            if index % 100000 == 0 and index != 0:
                print("processing No.%d chunk %d / %d" % (i, index, len(chunk)), len(drop_list))

        chunk.insert(chunk.shape[1], 'RECV_TIME', recv_time)
        chunk = chunk.drop(drop_list)
        chunk.to_csv('/Users/mulan/Documents/source_bike/chunk_slim/chunk' + str(i) + '.csv', index=False)
        # out_data = out_data.append(chunk)
        i += 1
    # out_data.to_csv('/Users/mulan/Documents/bike_io_source_data.csv', index=False)


def count_uniqueBike():
    df = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv')
    count = 0
    pre_identity = ""
    for index, row in df.iterrows():
        if row['IDENTITY_NO'] != pre_identity:
            count += 1
        pre_identity = row['IDENTITY_NO']
        if index % 100000 == 0 and index != 0:
            print("processing %d / %d" % (index, len(df)))
            # print(pre_identity, pre_monitor, len(drop_list))
    print(count)


def drop_overlapping():
    df = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv',
                     parse_dates=['STATS_TIME', 'RECV_TIME'])

    drop_list = []
    index = 0
    threshold = 6
    while True:
        if index >= len(df) - threshold:
            break
        bike_id = df.iloc[index]['IDENTITY_NO']
        if bike_id != df.iloc[index + 1]['IDENTITY_NO']:
            index += 1
            continue

        monitor1 = df.iloc[index]['MONITOR_ID']
        monitor2 = df.iloc[index + 1]['MONITOR_ID']
        check_point = index + 1
        while True:
            check_point += 1
            if df.iloc[check_point]['IDENTITY_NO'] != bike_id or check_point >= len(df) or (
                    df.iloc[check_point]['MONITOR_ID'] != monitor1 and df.iloc[check_point]['MONITOR_ID'] != monitor2):
                break

        if check_point > index + threshold:
            drop_list += range(index + 1, check_point)
        index = check_point - 1
        if index % 1000 == 0 and index != 0:
            print("processing %d / %d" % (index, len(df)), len(drop_list))

        index += 1
    df = df.drop(drop_list)
    df.to_csv('/Users/mulan/Documents/bike_NoAround_data.csv', index=False)


def drop_short():
    df = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv',
                     parse_dates=['STATS_TIME', 'RECV_TIME'])
    drop_list = []

    time_threshold = datetime.timedelta(days=0, hours=0, minutes=1)
    for index, row in df.iterrows():
        if row['RECV_TIME'] - row['STATS_TIME'] < time_threshold:
            drop_list.append(index)
    df = df.drop(drop_list)
    print(len(drop_list))
    df.to_csv('/Users/mulan/Documents/bike_around_source_drop2_data.csv', index=False)


def drop_self():
    df = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv',
                     parse_dates=['STATS_TIME', 'RECV_TIME'])
    drop_list = []

    pre_identity = ""
    pre_monitor = ""
    for index, row in df.iterrows():
        if row['IDENTITY_NO'] == pre_identity and row['MONITOR_ID'] == pre_monitor:
            drop_list.append(index)
        pre_identity = row['IDENTITY_NO']
        pre_monitor = row['MONITOR_ID']
        if index % 100000 == 0 and index != 0:
            print("processing %d / %d" % (index, len(df)))
    df = df.drop(drop_list)
    df.to_csv('/Users/mulan/Documents/bike_NoSelf_NoShort_NoAround_data2.csv', index=False)


def show_basic():
    in_flow = [[[0 for h in range(24)] for d in range(31)] for m in range(12)]
    out_flow = [[[0 for h in range(24)] for d in range(31)] for m in range(12)]

    df = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv',
                     parse_dates=['STATS_TIME', 'RECV_TIME'])
    for index, row in df.iterrows():
        in_flow[int(row['STAT S_TIME'].month) - 1][int(row['STATS_TIME'].day) - 1][int(row['STATS_TIME'].hour)] += 1
        out_flow[int(row['RECV_TIME'].month) - 1][int(row['RECV_TIME'].day) - 1][int(row['RECV_TIME'].hour)] += 1
        if index % 100000 == 0 and index != 0:
            print("processing %d / %d" % (index, len(df)))

    for m in range(12):
        if not os.path.exists('/Users/mulan/Documents/bike/month%d' % (m + 1)):
            os.makedirs('/Users/mulan/Documents/bike/month%d' % (m + 1))
        for d in range(31):
            inout_data = pd.DataFrame({'in_flow': in_flow[m][d], 'out_flow': out_flow[m][d]})
            inout_data.to_csv('/Users/mulan/Documents/bike/month%d/day%d.csv' % (m + 1, d + 1), index=False)


def drop_chunk(id):
    similar_window = 2
    head = ['ACCESS_ID', 'IDENTITY_NO', 'MONITOR_ID', 'MONITOR_NAME', 'STATS_TIME', 'DB_UPDATE_TIME', 'RECV_TIME']
    out_data = pd.DataFrame(columns=head)

    drop_list = []
    index = 0
    threshold = 4
    chunk = pd.read_csv("/Users/mulan/Documents/source_bike/chunk_slim/chunk" + str(id) + ".csv")
    while True:
        if index >= len(chunk) - threshold:
            break
        bike_id = chunk.iloc[index]['IDENTITY_NO']
        if bike_id != chunk.iloc[index + 1]['IDENTITY_NO']:
            index += 1
            continue

        check_point = index
        monitor = ["" for _ in range(similar_window)]
        while True:
            check_point += 1
            if check_point >= len(chunk) - 1 or chunk.iloc[check_point]['IDENTITY_NO'] != bike_id:
                break
            i = 0
            while i < similar_window:
                if monitor[i] == "":
                    monitor[i] = chunk.iloc[check_point]['MONITOR_ID']
                    break
                if monitor[i] == chunk.iloc[check_point]['MONITOR_ID']:
                    break
                i += 1
            if i == similar_window:
                break
        if check_point > index + threshold:
            drop_list += range(index + 1, check_point)
            chunk.loc[index, 'RECV_TIME'] = chunk.loc[check_point, 'RECV_TIME']
            index = check_point - 1
        if index % 1000 == 0 and index != 0:
            print("processing No.%d chunk %d / %d" % (id, index, len(chunk)), len(drop_list))

        index += 1
    chunk_2 = chunk.drop(drop_list)
    out_data = out_data.append(chunk_2)
    out_data.to_csv('/Users/mulan/Documents/source_bike/chunk_drop/bike_chunk' + str(id) + '_drop.csv', index=False)


def merge_remote():
    df = pd.read_csv('/data/DiskData/bike_3-5/drop_6/chunk0.csv')
    for i in range(1, 24):
        tmp = pd.read_csv('/data/DiskData/bike_3-5/drop_6/chunk' + str(i) + '.csv')
        df = df.append(tmp, ignore_index=True)
    df.to_csv('/data/DiskData/bike_3-5/final.csv', index=False)


def slim_remote(thread_no):
    df = pd.read_csv('/data/DiskData/bike_jinrongjie_3-5_source/slim_chunk' + str(thread_no) + '.csv')

    drop_list = []
    pre_index = -1
    pre_id = ""
    pre_monitor = ""
    recv_time = ["" for _ in range(len(df))]
    for index, row in df.iterrows():
        if index == len(df) - 1:
            recv_time[pre_index] = df.iloc[index - 1]['STATS_TIME']
            drop_list += list(range(pre_index + 1, index + 1))
            continue
        if row['IDENTITY_NO'] == pre_id:
            if row['MONITOR_ID'] == pre_monitor:
                continue
            elif row['MONITOR_ID'] != pre_monitor:
                if index > pre_index + 1:
                    drop_list += list(range(pre_index + 1, index))
                recv_time[pre_index] = df.iloc[index - 1]['STATS_TIME']
                pre_index = index
                pre_monitor = row['MONITOR_ID']
        else:
            if pre_index != -1:
                if index > pre_index:
                    drop_list += list(range(pre_index, index))
                recv_time[pre_index] = df.iloc[index - 1]['STATS_TIME']
            pre_index = index
            pre_id = row['IDENTITY_NO']
            pre_monitor = row['MONITOR_ID']
        if index % 100000 == 0 and index != 0:
            print("processing %d / %d" % (index, len(df)), len(drop_list))

    df.insert(df.shape[1], 'RECV_TIME', recv_time)
    df = df.drop(drop_list)
    df.to_csv('/data/DiskData/bike_jinrongjie_3-5_slim/chunk' + str(thread_no) + '.csv',
              index=False)


def split_task():
    df = pd.read_csv('/data/DiskData/bike_3-5/drop.csv')
    split_num = 24
    args = [0 for _ in range(split_num)]
    for i in range(split_num):
        if i == 0:
            continue
        index = int(len(df) / split_num * i)
        pre_id = df.iloc[index]['IDENTITY_NO']
        while True:
            if df.iloc[index]['IDENTITY_NO'] == pre_id:
                index += 1
            else:
                break
        args[i] = index

    for i in range(split_num - 1):
        print(args[i], args[i + 1])
        df[args[i]:args[i + 1]].to_csv('/data/DiskData/bike_jinrongjie_3-5_source/slim_chunk' + str(i) + '.csv',
                                       index=False)
    df[args[-1]:len(df)].to_csv('/data/DiskData/bike_jinrongjie_3-5_source/slim_chunk' + str(split_num - 1) + '.csv',
                                index=False)


def multiThread_drop(split_num):
    process_list = []
    for i in range(split_num):
        p = mp.Process(target=drop_remote, args=(i,))
        p.start()
        process_list.append(p)

    for p in process_list:
        p.join()


def drop_remote(thread_no):
    df = pd.read_csv('/data/DiskData/bike_3-5/drop_5/chunk' + str(thread_no) + '.csv')
    drop_list = []
    window = 6
    index = 0
    while index < len(df):
        if index % 10000 == 0:
            print("Thread No.%d: processing %d / %d" % (thread_no, index, len(df)), len(drop_list))
        check_point = index + 1
        isRec = True
        while True:
            if check_point > index + window:
                index += 1
                isRec = False
                break
            if check_point >= len(df) or df.iloc[check_point]['IDENTITY_NO'] != \
                    df.iloc[index]['IDENTITY_NO']:
                index = check_point
                isRec = False
                break
            if df.iloc[check_point]['MONITOR_ID'] == df.iloc[index]['MONITOR_ID']:
                break
            check_point += 1
        if not isRec:
            continue

        pre_point = check_point
        while check_point < len(df):
            if check_point > pre_point + window or df.iloc[check_point]['IDENTITY_NO'] != \
                    df.iloc[index]['IDENTITY_NO']:
                break
            if df.iloc[check_point]['MONITOR_ID'] == df.iloc[index]['MONITOR_ID']:
                pre_point = check_point
            check_point += 1

        window_length = check_point - index
        stations = ["" for _ in range(window_length)]
        for i in range(index, check_point):
            stations[i - index] = df.iloc[i]['MONITOR_ID']
        while check_point < len(df) and df.iloc[check_point]['IDENTITY_NO'] == df.iloc[index]['IDENTITY_NO']:
            isRec = False
            for i in range(len(stations)):
                if df.iloc[check_point]['MONITOR_ID'] == stations[i]:
                    isRec = True
                    break
            if not isRec:
                break
            check_point += 1
        drop_list += list(range(index + 1, check_point))
        df.loc[index, 'RECV_TIME'] = df.loc[check_point - 1, 'RECV_TIME']
        index = check_point

    df = df.drop(drop_list)
    df.to_csv('/data/DiskData/bike_3-5/drop_6/chunk' + str(thread_no) + '.csv', index=False)


def drop_final():
    df = pd.read_csv("/data/DiskData/bike_3-5/drop.csv")
    drop_list = []
    window = 8
    index = 0
    while index < len(df):
        check_point = index + 1
        count = window
        while check_point < len(df):
            if count < 0:
                break
            check_point += 1
        index += 1

    df = df.drop(drop_list)
    df.to_csv("/data/DiskData/bike_3-5/final.csv", index=False)


def random_collect():
    source_path = "/data/DiskData/一卡通滴滴数据/市经济信息化局楼宇到达订单数据/"
    out_head = ['date', 'time',
                'start_loc', 'start_lng', 'start_lat',
                'end_loc', 'end_lng', 'end_lat']
    out_data = [pd.DataFrame(columns=out_head) for _ in range(24)]
    for p in os.listdir(source_path):
        if len(p) > 20 and p[0:37] == "didi_daodaxicheng_building_arrive2020":
            df = pd.read_excel(source_path + p)
            index = 0
            while index < len(df):
                row = df.iloc[index]
                start_lng, start_lat = MarsStr2BMap(row["上车地lng"], row["上车地lat"])
                end_lng, end_lat = MarsStr2BMap(row["目的地lng"], row["目的地lat"])
                tmp = pd.DataFrame(
                    data=[[row["日期"], row["时间段"],
                           row["上车楼宇名称"], start_lng, start_lat,
                           row["目的地名称"], end_lng, end_lat]],
                    columns=out_head)
                out_data[int(row["时间段"])] = out_data[int(row["时间段"])].append(tmp, ignore_index=True)
                index += 37
            print("finish " + p, len(out_data[18]))
            if len(out_data[18]) > 500:
                break
    for i in range(24):
        out_data[i].to_csv("/data/DiskData/didi_collect/daoda_random37/" + str(i) + ".csv", index=False)


def 

if __name__ == '__main__':
    random_collect()
