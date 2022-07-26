from tkinter import Y
from comparator import Comparator
from exporter import Exporter
from datetime import datetime
from key_updater import KeyUpdater

# Updates all the primary and composite keys in the table
# keyUpdater = KeyUpdater()
# keyUpdater.updateKeys()

start_time = datetime.now()
# Input (lower-case) names of two environments to compare (do not mix up source/target)
source_env = "ps2"
target_env = "sit"

# naming for 
source_env_naming = "ps2"
target_env_naming = "sit"

# full table list
use_full_table_list = False

# Export tables from desired environment, comment out if you dont need to export
exporter = Exporter()
source_filepath = exporter.export(source_env,source_env_naming,use_full_table_list)
target_filepath = exporter.export(target_env,target_env_naming,use_full_table_list)

# uncomment these and put path if you dont need to export
# source_filepath = "../data_ps2_20072022_957pm/"
# target_filepath = "../data_ps2_20072022_957pm/"

# Compare the exported tables
comparator = Comparator()
comparator.compare(source_filepath, target_filepath, source_env_naming, target_env_naming)