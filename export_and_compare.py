from comparator import Comparator
from exporter import Exporter
from datetime import datetime

start_time = datetime.now()
# Input (lower-case) names of two environments to compare (do not mix up source/target)
source_env = "ps2"
target_env = "ps2"

# naming for 
source_env_naming = "mvp45_ps2"
target_env_naming = "mvp4_ps2"

# Export tables from desired environment, comment out if you dont need to export
exporter = Exporter()
source_filepath = exporter.export(source_env,source_env_naming)
# target_filepath = exporter.export(target_env,target_env_naming)

# uncomment these and put path if you dont need to export
# source_filepath = "../data_ps2_20072022_957pm/"
target_filepath = "../data_ps2_20072022_957pm/"

# Compare the exported tables
comparator = Comparator()
comparator.compare(source_filepath, target_filepath, source_env_naming, target_env_naming)