"""
This module provides all the mixins used within the builder classes and provides them the convenience for creating
multiple common interfaces and builder methods without being DRY.
"""

import inspect
import shutil
import warnings
import zipfile
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import DynamicClassAttribute
from types import ModuleType
from types import SimpleNamespace
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union

from loguru import logger
from typing_extensions import override
from typing_extensions import Self

from xflow.context import ParamStoreKey
from xcomms.logger import pformat
from xnl.metadata.column import ColumnType

T = TypeVar("T")
Deferred = Union[SimpleNamespace, T]


def _is_attr_deferred(attr: Any) -> bool:
    """
    Determines whether an input attribute is an instance of the "SimpleNamespace" class.

    :param attr: class instance
    :return: 'True' if attr is an instance of SimpleNamespace, 'False' otherwise.
    """
    return isinstance(attr, SimpleNamespace)


def _is_attr_empty(attr: Any) -> bool:
    """
    Check if the input attribute is deferred and lacks "value" or is None.

    :param attr: Attribute for checking.
    :return: 'True' if attribute is empty, 'False' otherwise.
    """
    return not hasattr(attr, "value") if _is_attr_deferred(attr) else attr is None


def _get_attr_name(obj: object, attr: Any) -> str:
    """
    Returns the name of an attribute within an object which has the same exact value.

    :param obj: An instance of a class
    :param attr: The value to match for in an attribute in the class
    :raises ValueError: no/multiple attributes match with the specified value
    :return: name of the attribute
    """
    attr_name = [name for name, value in vars(obj).items() if id(value) == id(attr)]
    if len(attr_name) != 1:
        raise ValueError(
            f"There's no/multiple attributes with the same value, cannot retrieved attribute name: {attr_name}"
        )

    return attr_name[0]


class CopyOnWriteMixin:
    """
    Enables the Copy-on-Write mechanism within builder classes to avoid excessive memory usage while still having
    immutability once changes has been made to the object.
    """

    def __init__(self):
        super().__init__()
        self._reuse: bool = False

    def copy_on_write(self: T, attr: Any, new_value: Any) -> T:
        """
        Modifies an object's attributes while preserving the original object's state by creating a new object
        only when necessary.

        :param attr: Attribute to be modified.
        :param new_value: New value for the set attribute.
        :raises ValueError: If attribute is None, error raised containing the instructions how to set it.
        :return: Modified object which can either be the original object or the modified object if conditions were met.
        :rtype: Self
        """
        if attr is None:
            raise ValueError(
                "The attribute is currently set to None, either set it as a Deferred variable by giving it"
                "a value of SimpleNamespace() or set any arbitrary value"
            )

        obj = self
        current_value = attr
        is_list = isinstance(current_value, list)
        is_dict = isinstance(current_value, dict)
        is_function = callable(current_value) and inspect.isfunction(current_value)
        attr_name = _get_attr_name(self, attr)
        logger.trace(f"Setting attribute {attr_name}={pformat(current_value)} with new value of {pformat(new_value)}")

        if self.is_reusable and (is_function or is_list or is_dict or current_value != new_value):
            logger.trace(f"Initiating deepcopy for {attr_name}")
            logger.trace(f"Old object: {pformat(self)}")

            obj = deepcopy(self)
            obj._reuse = False
            logger.trace(f"New object: {pformat(obj)}")

        attr = getattr(obj, attr_name)
        if is_list:
            if isinstance(new_value, list):
                attr.extend(new_value)
            else:
                attr.append(new_value)
        elif is_dict:
            attr.update(new_value)
        else:
            if isinstance(new_value, bool):
                new_value = SimpleNamespace(value=new_value)
            setattr(obj, attr_name, new_value)

        return obj

    def reuse(self) -> Self:
        """
        Objects will gain permanent immutability once it has been `reused`.

        :return: Self.
        """
        self._reuse = True
        return self

    @property
    def is_reusable(self) -> bool:
        """
        Check whether the object is already `reusable` or not.

        :return: 'True' if the object is marked for reuse, 'False' otherwise.
        """
        return self._reuse


@dataclass
class RequiredAttr:
    """
    Represent attributes within the builder classes that are required to be provided.
    """

    PROVIDED = "provided"
    """Flag to indicate that the value has already been provided in the :class:`~JobContext`."""

    obj: object
    """The object where the attribute exists."""

    attr: Any
    """The attribute's value."""

    setter: Callable
    """The attribute's setter function if there is any."""


class RequiredAttrMixin:
    """
    Responsible for checking and raising errors on object attributes that does not have a given value.
    """

    def __init__(self):
        self._properties: Deferred[List[str]] = SimpleNamespace()

    @property
    def properties(self) -> List[Tuple[str, property]]:
        """
        Get a list of properties defined in the class.

        :return: A list of property name-property pairs
        """
        if _is_attr_empty(self._properties):
            self._properties = inspect.getmembers(self.__class__, lambda x: isinstance(x, property))

        return self._properties

    @property
    def readonly_properties(self) -> List[str]:
        """
        Retrieves a list of read-only property names.

        :return: A list of read-only property names.
        """
        return [name for name, p in self.properties if p.fset is None]

    @property
    def writeable_properties(self) -> List[str]:
        """
        Retrieves a list of writeable property names.

        :return: A list of writeable property names.
        """
        return [name for name, p in self.properties if p.fset is not None]

    def required_attrs(self) -> List[RequiredAttr]:
        """
        Provides the attributes which are needed to be checked before each job run and must be overriden.

        :return:
        """
        return []

    def _check_attrs(self):
        """
        Checks if the required attributes specified for the class are present and have non-empty values.
        If any required attribute is missing or has an empty value, an error message is raised.

        :return: A list of error messages indicating the missing or empty required attributes.
        """
        error_messages = []
        required_attrs = self.required_attrs()
        logger.trace(f"Required attrs for class {self.__class__.__name__}: {pformat(required_attrs)}")

        for attr in required_attrs:
            if not _is_attr_empty(attr.attr) or attr.attr == RequiredAttr.PROVIDED:
                continue

            attr_name = _get_attr_name(attr.obj, attr.attr)
            error_messages.append(
                f"The {self.__class__.__name__}.{attr_name} property returns a NoneType. "
                f"Set it by using the {attr.setter.__name__}() method"
                + (
                    ", or setting the instance's property with a value"
                    if attr_name in self.writeable_properties
                    else " "
                )
                + (
                    " ,or overriding the fget return value of the property through inheritance."
                    if attr_name in self.readonly_properties
                    else ""
                )
            )

        if len(error_messages) > 0:
            raise ValueError(";\n\t".join(error_messages))


class MultipleTableMixin(CopyOnWriteMixin):
    """
    Added to classes which supports one or multiple tables to work with.
    """

    def __init__(self):
        super().__init__()
        self._tables: List[str] = []

    def add_table(self, table: Union[str, List]) -> Self:
        """
        Add one or more table names to the existing list of tables.

        :param table: A single or list of table names to add.
        :return: A new instance with the added table name/s.
        """
        return self.copy_on_write(self._tables, table)


class DatabaseNameMixin(CopyOnWriteMixin, RequiredAttrMixin):
    """
    Added to classes which requires a database name.
    """

    def __init__(self):
        super().__init__()
        self.db_name: Deferred[ParamStoreKey] = SimpleNamespace()

    def with_db_name(self, config_db_name: ParamStoreKey) -> Self:
        """
        Sets the database name to use.

        :param config_db_name: The database name.
        :return: A modified object with the updated 'db_name' attribute.
        """
        return self.copy_on_write(self.db_name, config_db_name)

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.db_name, self.with_db_name)]


class DatabaseDatamartMixin(MultipleTableMixin, DatabaseNameMixin):
    """
    Added to classes that requires a datamart location to be provided.
    """

    def __init__(self):
        super().__init__()
        self.dm_path: Deferred[ParamStoreKey] = SimpleNamespace()

    def with_dm_path(self, config_dm_path: ParamStoreKey) -> Self:
        """
        Sets the datamart location path to use.

        :param config_dm_path: The datamart location.
        :return: A modified object with the updated 'dm_path' attribute
        """
        return self.copy_on_write(self.dm_path, config_dm_path)

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.dm_path, self.with_dm_path)]


class ExternalFileLoaderMixin(CopyOnWriteMixin):
    """
    Added to classes which are required to load non-python files or external ones.
    """

    def __init__(self):
        super().__init__()
        self.file_loader_path: Deferred[Path] = self.LOADER_PATH
        self.file_type: Deferred[str] = SimpleNamespace()
        self.filenames: List[str] = []

    def _init_loader(self, file_type: str) -> Self:
        """
        Initialize a loader with a specific file type.

        :param file_type: Type of file to be used by the loader.
        :return: Modified object with the 'file_type' attribute updated.
        """
        return self.copy_on_write(self.file_type, file_type)

    def with_filename(self, filename: Union[List, str]) -> Self:
        """
        Sets a filename(s) to load.

        :param filename: Either a single filename(str) or list of filenames.
        :return: Modified object with updated filenames attribute.
        """
        filenames = filename
        if isinstance(filenames, str):
            filenames = [filenames]

        filenames = [f"{f}.{self.file_type}" for f in filenames]

        return self.copy_on_write(self.filenames, filenames)

    @property
    def loaded_files(self) -> List[str]:
        """
        Return the content of the files loaded from the loader path.

        :return: A list of strings containing the content of the loaded files.
        """
        logger.info(f"Loading files from {self.file_loader_path}: {self.filenames}")
        return [
            self.file_loader_path.joinpath(filename).read_text()  # pylint: disable=unspecified-encoding
            for filename in self.filenames
        ]

    @classmethod
    def set_loader_path(cls, root_module: Union[Path, ModuleType], dir_name: str = None, is_external: bool = False):
        """
        Sets the path where to load the files and is required to be called prior to creating an instance.

        :param root_module: The path or package where the files are located.
        :param dir_name: The name of the directory.
        :param is_external: Whether the files are coming from an external source or not.
        :return:
        """
        if isinstance(root_module, Path):
            cls.LOADER_PATH = root_module
            return

        if is_external:
            cls.LOADER_PATH = Path(root_module.__file__).parents[2].joinpath(dir_name)
        else:
            cls.LOADER_PATH = ExternalLoader(root_module, dir_name).path


class SingleExtFileLoaderMixin(ExternalFileLoaderMixin):
    """
    Added to classes that only require a single file to load.
    """

    @override
    def with_filename(self: T, filename: str) -> T:
        if not isinstance(filename, str):
            raise ValueError(
                f"{self.__class__.__name__} {self.with_filename.__name__} only accepts a str as an argument"
            )

        obj = super().with_filename(filename)

        # prevent for multiple filenames to accumulate by getting only the latest value inserted to the list
        if len(obj.filenames) > 1:
            obj.filenames = obj.filenames[-1:]

        return obj

    @override
    @property
    def loaded_files(self) -> str:
        return super().loaded_files[0]


class SingleFunctionMixin(CopyOnWriteMixin, RequiredAttrMixin):
    """
    Added to class which only supports single function to run and does not support function stacking.
    """

    def __init__(self):
        super().__init__()
        self._main_fn: Deferred[Callable[..., Any]] = SimpleNamespace()

    def with_main_fn(self, function: Callable[..., Any]) -> Self:
        """
        Sets the main function to be executed.

        :param function: The function to be used.
        :return:
        """
        return self.copy_on_write(self._main_fn, function)

    @property
    def main_fn(self) -> Callable[..., Any]:
        """
        Retrieves the wrapped function inside the object.

        :return:
        """
        return self._main_fn

    @main_fn.setter
    def main_fn(self, value: Callable[..., Any]):
        if not self.is_reusable and not _is_attr_empty(self._main_fn):
            warnings.warn(
                f"Builder object's main function has been overwritten from {self._main_fn} to {value.__name__}."
            )
        self._main_fn = value

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.main_fn, self.with_main_fn)]


class PipelineFunctionMixin(CopyOnWriteMixin):
    """
    Added to classes which support function stacking and creates a pipeline by adding the option to add pre- and
    post-processing functions between the main given function.
    """

    def __init__(self):
        super().__init__()
        self._pre_processing_fns: List[Callable[..., Dict[str, Any]]] = []
        self._post_processing_fns: List[Callable[..., Any]] = []

    def add_pre_processing(self, function: Union[List, Callable[..., Dict[str, Any]]]) -> Self:
        """
        Adds a pre-processing function to the pipeline.

        :param function: The pre-processing function to add.
        :return: A copy of the object with the added pre-processing function.
        """
        return self.copy_on_write(self._pre_processing_fns, function)

    def add_post_processing(self, function: Union[List, Callable[..., Any]]) -> Self:
        """
        Adds a post-processing function to the pipeline.

        :param function: The post-processing function to add.
        :return: A copy of the object with the added post-processing function.
        """
        return self.copy_on_write(self._post_processing_fns, function)


class ResultNameMixin(CopyOnWriteMixin, RequiredAttrMixin):
    """
    Added to classes to enable assigning of variable named added to the :class:`~JobContext`.
    """

    def __init__(self):
        super().__init__()
        self.result_name: Deferred[str] = SimpleNamespace()

    def with_result_name(self, result_name: str) -> Self:
        """
        Set the result name of the returned object from the function.

        :param result_name: The result name to set.
        :return: A copy of the object with the set result name.
        """
        return self.copy_on_write(self.result_name, result_name)

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.result_name, self.with_result_name)]


class ExternalLoader:  # pylint: disable=too-few-public-methods
    """
    Loads and extract modules from zip files.

    :param root_package: The source root package from which to load external code.
    :param dir_name: Name of directory containing the external resource to load.
    :param parent_package: An optional parent package if applicable.
    """

    def __init__(self, root_package: ModuleType, dir_name: str, parent_package: ModuleType = None):
        if parent_package is None:
            parent_package = root_package

        zipped_package_path = Path(root_package.__file__).parents[1].absolute()
        extract_path = zipped_package_path.parent
        file_name = zipped_package_path.joinpath(Path(parent_package.__file__).parent.joinpath(dir_name)).relative_to(
            zipped_package_path
        )

        zipped_package_path = str(zipped_package_path)
        is_zipfile = zipped_package_path.endswith(".zip")

        if is_zipfile:
            logger.info(f"Extracting {str(file_name)} from {str(zipped_package_path)} to {extract_path}")
            with zipfile.ZipFile(zipped_package_path, "r") as zipped_package:
                zipped_package.extractall(path=str(extract_path))

        loader_path = extract_path.joinpath(dir_name)
        if is_zipfile:
            shutil.move(extract_path.joinpath(file_name), loader_path)
            shutil.rmtree(zipped_package_path.replace(".zip", ""))

        logger.info(f"External loader path set: {loader_path}")
        self._path = loader_path

    @property
    def path(self) -> Path:
        """
        Get the path to the loaded external package.

        :return: The path to the loaded external package.
        """
        return self._path


class LowerCaseEnum(Enum):
    @DynamicClassAttribute
    def name(self):  # pylint: disable=function-redefined
        """The name of the Enum member."""
        return self._name_.lower()


class Scd2Columns(LowerCaseEnum):
    ROW_EFF_START_DATE = {"id": 2, "type": "date", "length": 0, "col_type": ColumnType.DEFAULT}
    ROW_EFF_END_DATE = {"id": 3, "type": "date", "length": 0, "col_type": ColumnType.DEFAULT}
    CURRENT_ROW_FLAG = {"id": 4, "type": "string", "length": 1, "col_type": ColumnType.PARTITION}


class ControlColumns(LowerCaseEnum):
    ROW_HASH_VALUE = {"id": 1, "type": "string", "length": 75, "col_type": ColumnType.DEFAULT}
    CTRL_CREATE_USER_ID = {"id": 5, "type": "string", "length": 250, "col_type": ColumnType.DEFAULT}
    CTRL_CREATE_TS = {"id": 6, "type": "timestamp", "length": 36, "col_type": ColumnType.DEFAULT}
    CTRL_LAST_UPDATE_USER_ID = {"id": 7, "type": "string", "length": 250, "col_type": ColumnType.DEFAULT}
    CTRL_LAST_UPDATE_TS = {"id": 8, "type": "timestamp", "length": 36, "col_type": ColumnType.DEFAULT}


class CurrentFlag(Enum):
    YES = "Y"
    NO = "N"


class DeletedFlag(Enum):
    COLUMN_NAME = "deleted_flag"
    YES = "Y"
    NO = "N"
