from mrjob.job import MRJob
from geohash import encode
from heapq import heappush, heappushpop, heappop
import json


class MRQueryNN(MRJob):
    def configure_args(self):
        super(MRQueryNN, self).configure_args()
        self.add_passthru_arg('-l', '--location', type=str,
                              help='location to query')
        self.add_passthru_arg('-k', '--k', type=int, help='number of points')

    def mapper_init(self):
        self.lng, self.lat = list(map(float, self.options.location.split()))
        self.hash = encode(self.lat, self.lng)
        self.points = []

    def mapper(self, _, value: str):
        value = json.loads(value)
        hash = value['hash']
        # get the longest common prefix
        i = 0
        while i < len(hash) and hash[i] == self.hash[i]:
            i += 1

        if len(self.points) == self.options.k and -self.points[-1][0] > i:
            return

        for point in value['points']:
            lng, lat = point
            lng, lat = float(lng), float(lat)
            dist = (lng - self.lng)**2 + (lat - self.lat)**2
            if len(self.points) < self.options.k:
                heappush(self.points, (-i, -dist, point))
            else:
                heappushpop(self.points, (-i, -dist, point))

    def mapper_final(self):
        yield None, self.points

    def reducer_init(self):
        self.points = []

    def reducer(self, _, points):
        for point_list in points:
            for point in point_list:
                if len(self.points) < self.options.k:
                    heappush(self.points, point)
                else:
                    heappushpop(self.points, point)

    def reducer_final(self):
        for i in range(len(self.points)):
            _, dist, location = heappop(self.points)
            yield self.options.k - i, (-dist, location)


if __name__ == "__main__":
    MRQueryNN.run()
