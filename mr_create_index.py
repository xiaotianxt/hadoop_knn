from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from geohash import encode
import re

RE = re.compile(r"\d+\.\d+")


class MRIndexCreater(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, value: str):
        if value.startswith("WKT"):
            return
        lng, lat = RE.findall(value)
        # geohash
        geohash = encode(float(lat), float(lng))
        yield geohash, (lng, lat)

    def reducer(self, key, values):
        yield None, {"hash": key, "points": list(values)}


if __name__ == "__main__":
    MRIndexCreater.run()
