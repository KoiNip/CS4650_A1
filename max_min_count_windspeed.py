from mrjob.job import MRJob

#re is used to create a list of numbers we can compare to
#val[63,64] is a number that represents quality, we must ensure this number matches one of the numbers in QUALITY_RE
import re
QUALITY_RE = re.compile(r"[01459]")

class MaxMinCountWindspeed(MRJob):
    def mapper(self, _, line):
        val = line.strip()
        (wind_dir, temp, q_temp, q_wind_dir) = (val[60:63], val[87:92], val[92:93], val[63:64])
        if (temp != "+9999" and re.match(QUALITY_RE, q_temp) and wind_dir != "+999" and re.match(QUALITY_RE, q_wind_dir)):
            yield wind_dir, {"max":int(temp), "min":int(temp), "count":1}

    def reducer(self, key, values):
        count = 0
        max = 0
        min = 9999999
        for x in values:
            if x["max"] > max:
                max = x["max"]
            if x["min"] < min:
                min = x["min"]
            count += x["count"]
        yield key, {"low":min, "high": max, "count":count}

if __name__ == '__main__':
    MaxMinCountWindspeed.run()
    #May need to add --no-bootstrap-mrjob to command at end to get it to run
