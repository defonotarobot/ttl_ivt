from comparator import Comparator
from exporter import Exporter

# Input (lower-case) names of two environments to compare (do not mix up source/target)
source_env = "prod"
target_env = "gpe"

# Export tables from desired environment, comment out if you dont need to export
exporter = Exporter()
source_filepath = exporter.export(source_env)
target_filepath = exporter.export(target_env)

# uncomment these and put path if you dont need to export
# source_filepath = "../data_prod_21072022/"
# target_filepath = "../data_gpe_21072022/"

# Compare the exported tables
comparator = Comparator()
comparator.compare(source_filepath, target_filepath, source_env, target_env)