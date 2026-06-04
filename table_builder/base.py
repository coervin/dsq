"""
This module provides the building blocks for establishing different builder classes.
"""
import functools
import inspect
import itertools
from abc import ABC
from abc import abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from types import ModuleType
from types import SimpleNamespace
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from loguru import logger
from typing_extensions import Self
from typing_extensions import override

from xcomms.logger import pformat
from xcomms.utils import get_fn_name
from xflow.context import JobContext
from xflow.context import ParamStoreKey
from xflow.task import Task
from xflow.task import TaskFunction
from xflow.task import TaskMultiFunction
from xflow.task import TaskResultFunction
from xflow.task import TaskResultIterator
from xflow.task import TaskResultType
from xflow.task import TaskType
from xflow.task import TaskVoidFunction
from xnl.table_builder.mixin import CopyOnWriteMixin
from xnl.table_builder.mixin import Deferred
from xnl.table_builder.mixin import PipelineFunctionMixin
from xnl.table_builder.mixin import RequiredAttr
from xnl.table_builder.mixin import RequiredAttrMixin
from xnl.table_builder.mixin import ResultNameMixin
from xnl.table_builder.mixin import SingleFunctionMixin
from xnl.table_builder.mixin import _is_attr_deferred
from xnl.table_builder.mixin import _is_attr_empty

BaseBuilderT = TypeVar("BaseBuilderT")


@dataclass
class ReferenceKey:
    """
    Represent and works like a pointer that is used whenever there are renamed variables in the :class:`~JobContext`.
    """

    key_pointer: str
    """The variable to retrieve the actual value."""

    def get_value(self, ref_dict: Dict[str, Any]) -> Any:
        """
        Recursively retrieves the indicated value until it is no longer also a :class:`~ReferenceKey`.

        :param ref_dict: The dictionary of all available variables.
        :return:
        """
        value = ref_dict[self.key_pointer]
        return value.get_value(ref_dict) if isinstance(value, ReferenceKey) else value


TaskBuild = Union[Task, TaskFunction, TaskResultIterator]


class BaseBuilder(ABC, CopyOnWriteMixin, RequiredAttrMixin):
    """
    Base class for all builder classes.
    """

    def __init__(self):
        super().__init__()
        self._fn_kwargs: Dict[str, Any] = {}
        self._all_kwargs: Deferred[Dict[str, Any]] = SimpleNamespace()
        self._context: Deferred[JobContext] = SimpleNamespace()
        self._task_type: Deferred[TaskType] = SimpleNamespace()

    def _init_subclass(self, cls: Type[BaseBuilderT]) -> BaseBuilderT:
        logger.trace(f"Initializing subclass {cls} from {self.__class__}")
        new_obj = cls()

        for name, existing_obj_attr in self.__dict__.items():
            new_obj_attr = None

            # Reuse attribute must not be reset by its subclass
            if hasattr(new_obj, name) and name != "_reuse":
                new_obj_attr = getattr(new_obj, name)
                is_new_obj_attr_empty = _is_attr_empty(new_obj_attr) or new_obj_attr == [] or new_obj_attr == {}
                is_existing_obj_attr_empty = (
                    _is_attr_empty(existing_obj_attr) or existing_obj_attr == [] or existing_obj_attr == {}
                )

                if not is_new_obj_attr_empty and (
                    new_obj_attr == existing_obj_attr
                    or is_existing_obj_attr_empty
                    or (not is_existing_obj_attr_empty and isinstance(new_obj_attr, list))
                ):
                    logger.trace(
                        f"Skipping {name} attribute, retaining default value of {name}={pformat(new_obj_attr)}"
                    )
                    continue

            logger.trace(
                f"Setting attribute {name}={pformat(new_obj_attr)} "
                f"with new value of {pformat(existing_obj_attr)} to new object."
            )

            setattr(new_obj, name, existing_obj_attr)

        return new_obj

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        """Override the required attributes method.

        This method calls the parent class's required_attrs method and appends a RequiredAttr
        instance based on the task attribute.

        :returns: A list of required attributes including the modified task type attribute.
        :rtype: List[RequiredAttr]
        """
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.task_type, self.as_task)]

    @abstractmethod
    def _generate_task_fn(self) -> TaskFunction:
        # A function should be returned to be able to defer the "final" function definition
        # instead of defining it directly to generate_task_fn()
        pass

    def build(self, wrapped_fn: bool = True) -> TaskBuild:
        """Build a task based on provided configuration

        This method generates a task function and optionally wrapped with the specific task type
        including its attributes when 'wrapped_fn' is True. The result is also modified depending on
        the JobContext.

        :param wrapped_fn: Whether to wrap the generated function with the specified task type, defaults to True
        :type wrapped_fn: bool, optional
        :return: A TaskBuild object representing the built task
        :rtype: TaskBuild
        """
        self._check_attrs()
        task_fn = self._generate_task_fn()

        task_fn_name = get_fn_name(self.main_fn) if hasattr(self, "main_fn") else task_fn.__name__

        if task_fn_name.startswith("_"):
            task_fn_name = task_fn_name[1:]

        task_fn = (
            self.task_type(task_fn, task_name="_".join([self.__class__.__name__, task_fn_name]), from_builder=True)
            if wrapped_fn
            else task_fn
        )

        # Make the Task object attributes available as parameters to tasks instances
        if wrapped_fn:
            for name, value in task_fn.__dict__.items():
                setattr(self, name, value)

        if not _is_attr_empty(self._context):
            return task_fn(self._context)

        return task_fn

    def as_task(self, cls: TaskType) -> Self:
        """Update the task type attribute with a new value

        :param cls: The new task type to be assigned to the attribute.
        :type cls: TaskType
        :return: A new instance with the updated task type attribute.
        :rtype: Self
        """
        return self.copy_on_write(self._task_type, cls)

    def execute(self, context: JobContext) -> Self:
        """Execute the task with the given context

        :param context: The context data to be used for task execution.
        :type context: xflow.context.JobContext
        :return: The new instance with the updated context attribute.
        :rtype: Self
        """
        return self.copy_on_write(self._context, context)

    def execute_build(self, context: JobContext, wrapped_fn: bool = False) -> TaskBuild:
        """Execute a task with the given context and build it

        This method executes and builds a task given the context, optionally wrapping the function based on
        the 'wrapped_fn' parameter.

        :param context: The context data to be used for task execution.
        :type context: xflow.context.JobContext
        :param wrapped_fn: Whether to wrap the generated function with the specified task type, defaults to False.
        :type wrapped_fn: bool, optional
        :return: A Taskbuild object representing the executed and built task.
        :rtype: TaskBuild
        """
        return self.execute(context).build(wrapped_fn)

    def add_kwargs(self, **kwargs) -> Self:
        """Modifies the key arguments within a task function

        This method adds the provided key arguments to the existing key arguments
        within the modified object.

        :return: Modified object with the additional key arguments
        :rtype: Self
        """
        return self.copy_on_write(self._fn_kwargs, kwargs)

    def kwarg_refs(self, **kwargs) -> Self:
        """Apply keyword argument references to the task function

        :param kwargs: Keyword arguments to be applied as references to the task function.
        :rtype kwargs: dict
        :return: The updated object with the applied keyword argument references.
        :rtype: Self
        """
        return self.rename_kwargs(**kwargs)

    def rename_kwargs(self, **kwargs) -> Self:
        """Rename keyword arguments for the task function.

        This method renames keyword arguments by providing a mapping of old names to new names.
        It updates the keyword argument configuration in '_fn_kwargs' and returns the modified object.

        :return: Modified object with the renamed keyword arguments
        :rtype: Self
        """
        updated_fn_kwargs = {}
        for old_key, new_key in kwargs.items():
            updated_fn_kwargs[new_key] = ReferenceKey(old_key)

        return self.copy_on_write(self._fn_kwargs, updated_fn_kwargs)

    def _generate_all_kwargs(self, context: JobContext) -> Dict[str, Any]:
        """Generate a dictionary of keyword arguments for a task function.

        This method combines attributes from the provided context, attributes from the object,
        and attributes defined in '_fn_kwargs'.

        :param context: the context for the task execution
        :type context: xflow.context.JobContext
        :return: A dictionary of keyword arguments for the task function
        :rtype: Dict[str, Any]
        """
        #  TODO: Address str to datetime conversion for actual_date
        context_attrs = {name: val for name, val in context.__dict__.items() if not name.startswith("_")}
        logger.trace(f"Loaded context attrs: {pformat(context_attrs)}")
        logger.trace(f"Loaded context 'context_kwargs': {pformat(context.context_kwargs)}")

        task_attrs = {
            name: val.value if _is_attr_deferred(val) else val
            for name, val in self.__dict__.items()
            if not name.startswith("_") and not _is_attr_empty(val) and val != RequiredAttr.PROVIDED
        }
        logger.trace(f"Loaded task attrs: {pformat(task_attrs)}")

        kwargs = {}
        kwargs.update(context_attrs)
        kwargs.update(context.context_kwargs)
        kwargs.update(task_attrs)

        # Update kwargs with _fn_kwargs so ReferenceKey.get_value()
        #   would work for references that are also existing in _fn_kwargs
        # None values from _fn_kwargs with existing global values are not included to prevent overriding them
        task_fn_kwargs = {
            key: value
            for key, value in self._fn_kwargs.items()
            if value is not None or value is None and key not in kwargs
        }
        kwargs.update(task_fn_kwargs)
        logger.trace(f"Loaded task 'fn_kwargs': {pformat(task_fn_kwargs)}")

        ref_kwargs = {
            key: value.get_value(kwargs) for key, value in self._fn_kwargs.items() if isinstance(value, ReferenceKey)
        }
        kwargs.update(ref_kwargs)
        logger.trace(f"Renamed fn kwargs: {pformat(ref_kwargs)}")

        kwargs = {name: val.value if isinstance(val, ParamStoreKey) else val for name, val in kwargs.items()}
        kwargs["spark"] = context.spark
        self._all_kwargs = kwargs
        logger.debug(f"Generated task kwargs: {pformat(self.all_kwargs)}")

        return self.all_kwargs

    def _execute_subfunction(self, func: Callable[..., BaseBuilderT], args: Any = None) -> BaseBuilderT:
        """Execute the given function/s with only set of indicated relevant arguments.

        :param func: The callable object/function
        :type func: Callable[..., T]
        :param args: Additional arguments to be passed to the function, defaults to None
        :type args: Any, optional
        :return: The return value of the executed function
        :rtype: T
        """
        has_args = args is not None
        signature = inspect.signature(func)
        parameter_names = list(signature.parameters)

        first_parameter = None
        if len(parameter_names) > 0:
            first_parameter = parameter_names[0]
            if first_parameter == "self" and len(parameter_names) > 1:
                first_parameter = parameter_names[1]
            elif first_parameter == "self" and len(parameter_names) < 2:
                first_parameter = None

        fn_kwargs = {
            name: value
            for (name, value) in self.all_kwargs.items()
            if name in parameter_names
            and (has_args and first_parameter is not None and name != first_parameter or not has_args)
        }

        function_name = get_fn_name(func)
        logger.info(f"Executing {function_name}() with signature: {signature}")
        logger.info(
            f"Supplied arguments to {function_name}():"
            f"{' args: ' + pformat(args) + ', ' if has_args else ''}"
            f"'kwargs: '{pformat(fn_kwargs)}"
        )

        if has_args:
            if "self" in parameter_names:
                func = functools.partial(func, fn_kwargs.pop("self"))
            output = func(args, **fn_kwargs)
        else:
            output = func(**fn_kwargs)

        logger.debug(f"{function_name}() output: {pformat(output)}")

        return output

    @property
    def all_kwargs(self) -> Dict[str, Any]:
        """
        Returns all the keyword arguments provided to all the tasks in any given job.

        :return:
        """
        if _is_attr_empty(self._all_kwargs):
            raise ValueError(f"Call {self._generate_all_kwargs.__name__} to use the all_kwargs property")

        return self._all_kwargs

    @property
    def task_type(self) -> TaskType:
        """
        Check if task type is inherited.
        :return:
        """
        return self._task_type

    def __deepcopy__(self, memodict: Dict[int, Any]):
        """Generate a deep copy of the object.

        This method creates a new independent instance of the object's class.

        :param memodict: _description_
        :type memodict: Dict[int, Any]
        :return: _description_
        :rtype: _type_
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memodict[id(self)] = result

        for key, value in self.__dict__.items():
            # _all_kwargs is not included in deepcopy since it will always be generated upon
            #   the function build; also prevents non-pickable objects to be deepcopied
            if key == "_all_kwargs":
                value = SimpleNamespace()

            # Prevent ParamStoreKey to be deepcopied to prevent giving the
            #   builder class a new ParamStoreKey with a 'concrete' value of None.
            #   The ParamStoreKey key instance should always come from the Config singleton class.
            elif not isinstance(value, ParamStoreKey) and not (
                isinstance(value, list) and any(isinstance(i, ModuleType) for i in value)
            ):
                logger.trace(f"{self.__class__.__name__} builder deepcopy: {key}={value} of type {type(value)}")
                value = deepcopy(value, memodict)
            setattr(result, key, value)

        return result


class BaseOperationBuilder(BaseBuilder, SingleFunctionMixin):
    """A class for building operations or raw functions as tasks."""

    def _set_main_function(self: BaseBuilderT, function: Callable, **kwargs) -> BaseBuilderT:
        obj = self.add_kwargs(**kwargs)
        obj.main_fn = function
        return obj


class RawOperationBuilder(BaseOperationBuilder):
    """A class for building operations or raw functions as tasks."""

    def build(self, wrapped_fn: bool = True) -> TaskBuild:
        """Build a task based on the function

        :param wrapped_fn: A boolean indicating whether the function should be wrapped, defaults to True
        :type wrapped_fn: bool, optional
        :return: The built task
        :rtype: TaskBuild
        """
        task_fn = self._generate_task_fn()

        task_fn_name = get_fn_name(self.main_fn) if hasattr(self, "main_fn") else task_fn.__name__

        task_fn = self.task_type(task_fn, task_name="_".join([self.__class__.__name__, task_fn_name]))
        return task_fn

    @override
    def _generate_task_fn(self) -> TaskFunction:
        """Generate task function for executing operation.

        :return: The task function
        :rtype: TaskFunction
        """

        def task_fn(context: JobContext) -> TaskResultIterator:
            self._generate_all_kwargs(context)
            return self._execute_subfunction(self.main_fn)

        return task_fn


class SimpleVoidBuilder(BaseOperationBuilder):
    """A class for building tasks that do not return a value.

    It extends the 'BaseBuilder' and 'SingleFunctionMixin' classes with the functionality
    to building tasks without a return value
    """

    @override
    def _generate_task_fn(self) -> TaskVoidFunction:
        def task_fn(context: JobContext):
            self._generate_all_kwargs(context)
            self._execute_subfunction(self.main_fn)

        return task_fn


class SimpleReturnBuilder(SimpleVoidBuilder, ResultNameMixin):
    """A class for building tasks that return a value."""

    def __init__(self):
        super().__init__()
        self._result_type: Deferred[TaskResultType] = SimpleNamespace()

    def with_result_type(self, result_type: TaskResultType) -> Self:
        """
        Create a new builder with the specified task result type.

        :param result_type: The type of new task result
        :type result_type: TaskResultType
        :return: A new builder instance with the updated result type
        :rtype: Self
        """
        return self.copy_on_write(self._result_type, result_type)

    @override
    def _generate_task_fn(self) -> TaskResultFunction:
        """
        Generate a task function for executing the operation and producing the result.

        :return: The task function with the result_name and output of the operation
        :rtype: TaskResultFunction
        """

        def task_fn(context: JobContext) -> TaskResultIterator:
            self._generate_all_kwargs(context)
            output = self._execute_subfunction(self.main_fn)
            yield self.task_result_type(self.result_name, output)

        return task_fn

    @override
    def required_attrs(self) -> List[RequiredAttr]:
        """
        Get the list of required attributes, including the newly set result type.

        :return: List of required attributes
        :rtype: List[RequiredAttr]
        """
        attrs = super().required_attrs()
        return attrs + [RequiredAttr(self, self.task_result_type, self.with_result_type)]

    @property
    def task_result_type(self) -> TaskResultType:
        """
        Returns the default result type for the given builder class.

        :return:
        """
        return self._result_type


class PipelineBuilder(BaseOperationBuilder, PipelineFunctionMixin):
    """
    Class that enables assembling multiple functions as a pipeline.
    """

    def _update_all_kwargs(self, output, raise_error: bool = False) -> bool:
        """Check output and update it in the all_kwargs variable.

        This method checks the output of the function. If the output is a dictionary,
        it is added to the builder's 'all_kwargs'. If not, an error is raised

        :param output: The result of the function call
        :type output: Any
        :param raise_error: Boolean indicating if the output is a dictionary or not, defaults to False
        :type raise_error: bool, optional
        :raises ValueError: Function should return a dictionary
        :return: True if output is a dictionary, False if otherwise
        :rtype: bool
        """
        is_dict = False

        if isinstance(output, dict):
            self.all_kwargs.update(output)
            is_dict = True
        elif raise_error:
            raise ValueError("function output should return a dictionary")

        return is_dict

    def _main_fn_with_pre_processing(self, context: JobContext) -> Tuple[Any, Dict[str, Any]]:
        """Execute the main function with pre-processing functions.

        The method generates keyword arguments depending on the JobContext, iterating through a list of functions,
        executing each function, and updating a dictionary with the output if it's a dictionary.

        :param context: The job context for the task execution
        :type context: xflow.context.JobContext
        :return: A tuple containing the result of the main function and dictionary of pre-processing results
        :rtype: Tuple[Any, Dict[str, Any]]
        """
        self._generate_all_kwargs(context)

        output = {}
        for function in self._pre_processing_fns:
            pre_processing_output = self._execute_subfunction(function)
            if self._update_all_kwargs(pre_processing_output):
                output.update(pre_processing_output)

        return self._execute_subfunction(self.main_fn), output

    @abstractmethod
    def _generate_task_fn(self):
        pass


class PipelineVoidBuilder(SimpleVoidBuilder, PipelineBuilder):
    """
    Specific type of :class:`~PipelineBuilder` where the pipeline does not return anything.
    """

    @override
    def _generate_task_fn(self) -> TaskVoidFunction:
        def task_fn(context: JobContext):
            self._main_fn_with_pre_processing(context)

        return task_fn


class PipelineReturnBuilder(SimpleReturnBuilder, PipelineBuilder):
    """
    Specific type of :class:`~PipelineBuilder` where the pipeline should have a return value.
    """

    def _result_after_post_processing(self, output: Tuple[Any, Dict[str, str]]):
        """Apply post processing to the main output and create the final task result.

        :param output: A tuple containing the result of the main function and dictionary of pre-processing results.
        :type output: Tuple[Any, Dict[str, str]]
        :return: The final task result of the post processing functions
        :rtype: Taskresult
        """
        main_output, _ = output

        for function in self._post_processing_fns:
            main_output = self._execute_subfunction(function, main_output)
            self._update_all_kwargs(main_output, raise_error=False)

        return self.task_result_type(self.result_name, main_output)

    @override
    def _generate_task_fn(self) -> TaskResultFunction:
        def task_fn(context: JobContext) -> TaskResultIterator:
            output = self._main_fn_with_pre_processing(context)
            yield self._result_after_post_processing(output)

        return task_fn


def tuple_to_dict(*args) -> Dict[str, Any]:
    """Convert selected variables to a dictionary.

    A dictionary is created where the variable names act as keys and
    the content of each variable name serve as the value.

    :return: Variable names from the local variables of the calling function
    :rtype: Dict[str, Any]
    """
    transformed_dict = {}

    fn_vars = list(inspect.stack()[1].frame.f_locals.items())
    for var_name, var_value in fn_vars:
        if var_value not in args:
            continue

        transformed_dict[var_name] = var_value

    return transformed_dict


class MultiPipelineBuilder(BaseOperationBuilder):
    """This class extends the functionality of the input class with a generated function
    that ensures that the output is composed of iterators.

    :raises ValueError: If the main function's output is not an iterator
    :return: An iterator of task results
    :rtype: Iterator[Task]
    """

    @override
    def _generate_task_fn(self) -> TaskMultiFunction:
        def task_fn(context: JobContext) -> Iterator[Task]:
            self._generate_all_kwargs(context)
            self.all_kwargs.update({"context": context})
            output = self._execute_subfunction(self.main_fn)
            output, output2 = itertools.tee(output)

            if not all(isinstance(result, Iterator) for result in output2):
                raise ValueError(
                    f"The yielded value of the {self.main_fn.__name__}() function must also be a generator"
                )

            yield_flat = itertools.chain.from_iterable(list(result) for result in output)

            return yield_flat

        return task_fn
