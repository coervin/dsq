"""
Initializes the interface needed for the builder classes.
"""

from typing import Type

from xnl.table_builder.base import BaseBuilderT
from xnl.table_builder.cleanse import TableCleanser
from xnl.table_builder.create import TableInitializer
from xnl.table_builder.extract import TableExtractor
from xnl.table_builder.load import TableLoader
from xnl.table_builder.metadata import MetadataInitializer
from xnl.table_builder.mixin import ExternalFileLoaderMixin
from xnl.table_builder.transform import TableTransformer


class Table:
    """
    Class interface for accessing all builder classes for job composition.
    """

    @staticmethod
    def initializer():
        """
        :class:`~TableInitializer`

        :return:
        """
        return Table._create_builder(TableInitializer)

    @staticmethod
    def metadata():
        """
        :class:`~TableInitializer`

        :return:
        """
        return Table._create_builder(MetadataInitializer)

    @staticmethod
    def extractor():
        """
        :class:`~TableExtractor`

        :return:
        """
        return Table._create_builder(TableExtractor)

    @staticmethod
    def cleanser():
        """
        :class:`~TableCleanser`

        :return:
        """
        return Table._create_builder(TableCleanser)

    @staticmethod
    def transformer():
        """
        :class:`~TableTransformer`

        :return:
        """
        return Table._create_builder(TableTransformer)

    @staticmethod
    def loader():
        """
        :class:`~TableLoader`

        :return:
        """
        return Table._create_builder(TableLoader)

    @staticmethod
    def _create_builder(builder_cls: Type[BaseBuilderT]) -> BaseBuilderT:
        if issubclass(builder_cls, ExternalFileLoaderMixin) and (
            not hasattr(builder_cls, "LOADER_PATH") or builder_cls.LOADER_PATH is None
        ):
            raise ValueError(
                f"LOADER_PATH is not yet set for {builder_cls.__name__} using {builder_cls.set_loader_path.__name__}"
            )

        return builder_cls()
