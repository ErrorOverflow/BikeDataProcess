import pandas as pd
import os


def analysis_local(path):
    monitor_ids = get_all_monitorID()
    df = pd.read_csv(path)
    out_data = pd.DataFrame([[0 for column in range(130)] for index in range(130)], index=monitor_ids,
                            columns=monitor_ids)
    pre_id = df.loc[0, 'IDENTITY_NO']
    for index, row in df.iterrows():
        if index == 0 or row['IDENTITY_NO'] != pre_id:
            pre_id = row['IDENTITY_NO']
            continue

        out_data.loc[int(df.iloc[index - 1]['MONITOR_ID']), int(row['MONITOR_ID'])] += 1
        if index % 10000 == 0:
            print("processing %d / %d" % (index, len(df)))
        pre_id = row['IDENTITY_NO']
    out_data.to_csv("C:\\Users\\wml\\Desktop\\bike_matrix_3-5.csv")


def get_all_monitorID():
    df = pd.read_csv("C:\\Users\\wml\\Desktop\\bike_monitor_info.csv")
    monitor_id = list(df['MONITOR_ID'])
    return monitor_id


def build_string(start, end, num):
    generator = "\n\trandomCount = " + num + \
                "\n\twhile (randomCount--) {\n\t\tcurve.setOptions({\n\t\t\tstart: [" + start + \
                "],\n\t\t\tend: [" + end + \
                "]\n\t\t});\n\t\tvar curveModelData = curve.getPoints();\n\n\t\tdata.push({\n\t\t\tgeometry: {" \
                "\n\t\t\ttype: 'LineString',\n\t\t\t\tcoordinates: curveModelData\n\t\t\t},\n\t\t\tproperties: {" \
                "\n\t\t\t\tcount: Math.random()\n\t\t\t}\n\t\t});\n\t}\n "
    return generator


def generate_code():
    location = pd.read_csv("C:\\Users\\wml\\Desktop\\bike_monitor_info.csv", index_col=0)
    matrix = pd.read_csv("C:\\Users\\wml\\Desktop\\bike_matrix.csv", index_col=0)
    with open('C:\\Users\\wml\\Desktop\\test.txt', 'a+', encoding='utf-8') as f:
        i = 0
        for index, row in matrix.iterrows():
            j = 0
            for column, item in row.iteritems():
                if item <= 400:
                    j += 1
                    continue
                rec = ""
                start = str(location.loc[int(matrix.index[i])]['BAIDU_LONGITUDE']) + ", " + str(
                    location.loc[int(matrix.index[i])]['BAIDU_LATITUDE'])
                end = str(location.loc[int(matrix.columns[j])]['BAIDU_LONGITUDE']) + ", " + str(
                    location.loc[int(matrix.columns[j])]['BAIDU_LATITUDE'])
                num = str(int(item / 100))
                rec += build_string(start, end, num)
                f.write(rec)
                j += 1
            i += 1


def generate_head(f):
    head = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <title>MapVGL</title>
    <meta http-equiv="X-UA-Compatible" content="IE=Edge">
    <meta name="viewport" content="initial-scale=1.0, user-scalable=no">
    <style>
    html,
    body {
        width: 100%;
        height: 100%;
        margin: 0;
        padding: 0;
    }
    #map_container {
        width: 100%;
        height: 100%;
        margin: 0;
    }
    </style>
    <script src="//api.map.baidu.com/api?v=1.0&type=webgl&ak=1XjLLEhZhQNUzd93EjU5nOGQ"></script>
    <script src="//mapv.baidu.com/build/mapv.min.js"></script>
    <script src="https://mapv.baidu.com/gl/examples/static/common.js"></script>
    <script src="https://code.bdstatic.com/npm/mapvgl@1.0.0-beta.109/dist/mapvgl.min.js"></script>
    <script src="https://code.bdstatic.com/npm/mapvgl@1.0.0-beta.109/dist/mapvgl.threelayers.min.js"></script>
</head>
<body>
    <div id="map_container"></div>
    <script>

    var map = initMap({
        tilt: 45,
        heading: 0,
        center: [116.441646, 39.930206],
        zoom: 13,
        style: purpleStyle
    });

    var curve = new mapvgl.BezierCurve();
    
    var randomCount = 0;

    var data = [];
    """
    f.write(head)


def generate_end(f):
    end = """

    var view = new mapvgl.View({
        map: map
    });



    var flylineLayer = new mapvgl.FlyLineLayer({
        style: 'chaos',
        step: 0.1,
        color: 'rgba(33, 242, 214, 0.3)',
        textureColor: '#56ccdd',
        textureWidth: 20,
        textureLength: 60
    });

    view.addLayer(flylineLayer);
    flylineLayer.setData(data);
    </script>
</body>
</html>
    """
    f.write(end)


def generate_didi():
    path = "C:\\Users\\wml\\WebstormProjects\\untitled\\daoda_random37\\"
    files = os.listdir(path)
    for f in files:
        with open('C:\\Users\\wml\\WebstormProjects\\untitled\\daoda_result\\' + f + '.html', 'w', encoding='utf-8') as file:
            generate_head(file)
            df = pd.read_csv(path + f)
            for index, row in df.iterrows():
                rec = ""
                if row['start_lng'] > 0 and row['start_lat'] > 0 and row['end_lng'] > 0 and row['end_lat'] > 0:
                    start = str(row['start_lng']) + ", " + str(row['start_lat'])
                    end = str(row['end_lng']) + ", " + str(row['end_lat'])
                    num = str(1)
                    rec += build_string(start, end, num)
                    file.write(rec)

            generate_end(file)
            file.close()


def judge_location():
    location = pd.read_csv("C:\\Users\\wml\\Desktop\\bike_monitor_info.csv", index_col=0)
    for index, row in location.iterrows():
        if not (116.3631 < row['BAIDU_LONGITUDE'] < 116.3807 and 39.9130 < row['BAIDU_LATITUDE'] < 39.9301):
            print(row['MONITOR_NAME'], row['BAIDU_LONGITUDE'], row['BAIDU_LATITUDE'])


if __name__ == '__main__':
    generate_didi()
