


# Lets set up our imports 
from typing import Any, Tuple, Iterable


import apache_beam as beam


# This DoFn makes use of TimestampParam to access the elements timestamp
# We also make use of WindowParam to access the window that the element belongs to 
class GetElementTimestamp(beam.DoFn):
  def __init__(self, print_pane_info: bool = False):
    self.print_pane_info = print_pane_info
  
  def process(self, element: Any, 
              timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam,
              pane_info=beam.DoFn.PaneInfoParam
                )-> Iterable[Tuple[str,str]]:

    timestamp_str = ""
    try:
        timestamp_str = timestamp.to_utc_datetime()
    except:
        timestamp_str = timestamp
    
    window_str = ""

    if window == beam.window.GlobalWindow():
      window_str = "The Global Window"
    else: 
      window_str = f'Window ends at {window.max_timestamp().to_utc_datetime()}'

    if self.print_pane_info:
      yield (window_str , f'The value : {element} has timestamp {timestamp_str} with Pane {pane_info}')
    else: 
      yield (str({window_str}), f'The value : {element} has timestamp {timestamp_str}')


if __name__ == "__main__":

  our_values = [('A', 1.05), ('A', 1.02), ('A', 1.03), ('A', 1.04), ('A', 1.06), ('A', 1.07)]

  with beam.Pipeline() as p:
    _ = (p | beam.Create(our_values) 
          | beam.ParDo(GetElementTimestamp())
          | beam.Map(print)

    )

    global_window = beam.transforms.window.GlobalWindow()
