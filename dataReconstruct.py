import pandas as pd
import datetime

if __name__ == '__main__':
    head = ['ACCESS_ID', 'IDENTITY_NO', 'MONITOR_ID', 'MONITOR_NAME', 'STATS_TIME', 'RECV_TIME']
    out_data = pd.DataFrame(columns=head)
    df = pd.read_csv('/Volumes/disk/bike_access_around_jinrongjie.csv',
                     parse_dates=['STATS_TIME'])
    df['STATS_TIME'] = pd.to_datetime(df['STATS_TIME'])
    print(len(df))

    pre_id = ""
    pre_monitor = ""
    for index, row in df.iterrows():
        if row['IDENTITY_NO'] == pre_id and row['MONITOR_ID'] != pre_monitor:
            s = pd.Series([], index=head)
        pre_id = row["IDENTITY_NO"]
        pre_monitor = row["MONITOR_ID"]

    out_data.to_csv('/Users/mulan/Documents/bike_io_data.csv', index=False)
