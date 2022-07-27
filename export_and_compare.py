from tkinter import Y
from color import color
from comparator import Comparator
from exporter import Exporter
from datetime import datetime
from key_updater import KeyUpdater
import sys

# Updates all the primary and composite keys in the table

answer = input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "Please indicate if you'd like to update all keys (Y for yes, any other key for no): ")
if (answer.lower() == 'y'):
    keyUpdater = KeyUpdater()
    keyUpdater.updateKeys()

# Input (lower-case) names of two environments to compare (do not mix up source/target)
valid_envs = ["prod", "ps2", "sit", "uat", "gpe"]
while True:
    source_env = input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "Please input the first environment you'd like to compare: ").lower()
    if source_env in valid_envs:
        break
    else:
        print(color.RED + "Invalid environment name" + color.END)
while True:
    target_env = input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "Please input the second environment you'd like to compare: ").lower()
    if target_env in valid_envs:
        break
    else:
        print(color.RED + "Invalid environment name" + color.END)

# source_env = "ps2"
# target_env = "sit"

# naming for 
source_env_naming = source_env
target_env_naming = target_env

# full table list
use_full_table_list = input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "Would you like to compare from the full table list (press Y) or just the tables from table_list.txt (any other key): ").lower() == 'y'

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