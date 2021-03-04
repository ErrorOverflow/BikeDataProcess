import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt

if __name__ == '__main__':
    for m in range(12):
        if not os.path.exists('/Users/mulan/Documents/bike_image/month%d' % (m + 1)):
            os.makedirs('/Users/mulan/Documents/bike_image/month%d' % (m + 1))
        for d in range(31):
            df = pd.read_csv('/Users/mulan/Documents/bike_data/month%d/day%d.csv' % (m + 1, d + 1))
            in_flow = df['in_flow'].values.tolist()
            out_flow = df['out_flow'].values.tolist()

            fig = plt.figure()
            ax = plt.axes()

            plt.plot(range(0, 24), in_flow, color='b', marker='+', label='in')
            plt.plot(range(0, 24), out_flow, color='g', marker='+', label='out')

            plt.legend()
            plt.savefig('/Users/mulan/Documents/bike_image/month%d/day%d.png' % (m + 1, d + 1))
            plt.close()
