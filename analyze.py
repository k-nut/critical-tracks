import sqlite3
import json
import sys

FILENAME = "./criticaltracks.sqlite"


def get_distane(point, other_point):
    from math import sin, cos, sqrt, atan2, radians

    # approximate radius of earth in m
    R = 6373.0 * 1_000

    lon1, lat1 = [radians(x) for x in point["geometry"]["coordinates"]]
    lon2, lat2 = [radians(x) for x in other_point["geometry"]["coordinates"]]

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def may_show(point, points):
    NEIGHBORS = 3
    DISTANCE = 100
    found = 0
    for candidate in points:
        if get_distane(candidate, point) < DISTANCE:
            found += 1
        if found == NEIGHBORS:
            return True
    return False


def create_point(key, values):
    lat = values["latitude"] / 1_000_000
    lon = values["longitude"] / 1_000_000
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
    }


def main():
    con = sqlite3.connect(FILENAME)
    cur = con.cursor()
    rows = []
    for row in cur.execute('select * from tracks where timestamp < "2023-02-25"'):
        timestamp, data = row
        print(timestamp, file=sys.stderr)
        points = [create_point(key, value) for key, value in json.loads(data)['locations'].items()]
        filtered_points = [point for point in points if may_show(point, points)]
        if len(filtered_points):
            rows.append({"timestamp": timestamp, "data": filtered_points})
    print(json.dumps(rows))

if __name__ == "__main__":
    main()
