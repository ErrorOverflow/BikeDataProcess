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


if __name__ == '__main__':
    pass
