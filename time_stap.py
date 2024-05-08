
# Lets set up our imports 
import time


import apache_beam as beam

from basic import GetElementTimestamp


format = '%Y/%m/%d %H:%M:%S'
start = time.mktime(time.strptime('2000/01/01 10:00:00', format))

our_values = [('A', 1.05), ('A', 1.02), ('A', 1.04), ('A', 1.06)]

# This will create a Tuple of key, value, timestamp_as_date_str
our_values_with_timestamps = [beam.window.TimestampedValue(k, ( int(start + i))) for k, i in zip( our_values, range(0,len(our_values)))]
# Next we will add a value which is several seconds ahead of the last value shown above, this will be used later
our_values_with_timestamps.append(beam.window.TimestampedValue(('A',1.03), int(start+1)))
our_values_with_timestamps.append(beam.window.TimestampedValue(('A',1.07), int(start+8)))

our_values_with_timestamps.sort(key=lambda x:x.timestamp)

# for i in our_values_with_timestamps:
#   print(f' Value {i.value} has event timestamp {i.timestamp.to_utc_datetime()}')



# with beam.Pipeline() as p:
#     _ = (p| beam.Create(our_values_with_timestamps)
#           # | beam.Map(lambda x : x) # Work around for typing issue
#           | beam.WindowInto(beam.window.FixedWindows(1))
#           | beam.ParDo(GetElementTimestamp())
#           | beam.Map(print)
#     )


with beam.Pipeline() as p:
  _ = (p| beam.Create(our_values_with_timestamps)
         | beam.Map(lambda x : x) # Work around for typing issue
         | beam.WindowInto(beam.window.FixedWindows(1),timestamp_combiner=beam.window.TimestampCombiner.OUTPUT_AT_EARLIEST)
         | beam.combiners.Count.PerKey()  # this counts and keeps a key value pair, So, the data is reduced.
         | beam.Map(lambda x : f'Value: {x[0]} , Count: {x[1]}')
         | beam.ParDo(GetElementTimestamp())
         | beam.Map(print)
  )