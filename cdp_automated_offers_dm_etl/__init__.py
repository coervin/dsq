import sys
from xnl.table_builder.mixin import ExternalLoader
from xnl.table_builder.metadata import MetadataInitializer
from xnl.table_builder.transform import TableTransformer

TableTransformer.set_loader_path(ExternalLoader(sys.modules[__name__], "sql").path)
MetadataInitializer.set_loader_path(ExternalLoader(sys.modules[__name__], "schemas").path)
