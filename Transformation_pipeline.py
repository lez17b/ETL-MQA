import ETLcsv
from ETLcsv import ExtractTransformLoad

init_df = ExtractTransformLoad("file.csv")
# init_df.print_df()

# null handling pipelining
init_df = init_df.null_handler(['a', 'b', 'c', 'd', 'e', 'f', 'g'])

# Nan Handling pipeline
init_df = init_df.nan_handler(['a', 'b', 'c', 'd', 'e', 'f', 'g'])








