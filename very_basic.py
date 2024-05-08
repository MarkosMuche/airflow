# Lets set up our imports 


import apache_beam as beam

# In this cell we are going to run a simple pipeline that computes the Count of
# a list of values, which have the key 'A'. 

our_values = [('A', 1.05), ('A', 1.02), ('A', 1.03), ('A', 1.04), ('A', 1.06), ('A', 1.07), 
              ('B', 1.05), ('B', 1.02), ('B', 1.03), ('B', 1.04), ('B', 1.06)]

with beam.Pipeline() as p:
  _ = (p | beam.Create(our_values) 
         | beam.transforms.combiners.Count.PerKey()
         | beam.Map(lambda x : f'Key is {x[0]}, Count is {x[1]}')
         | beam.Map(print)
  )

