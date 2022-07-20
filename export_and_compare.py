from comparator import Comparator
from exporter import Exporter

source_env = "ps2"
target_env = "uat"

# Export tables from desired environment
exporter = Exporter()
source_filepath = exporter.export(source_env)
target_filepath = exporter.export(target_env)

# Compare the exported tables
comparator = Comparator()
comparator.compare(source_filepath, target_filepath, source_env, target_env)