"""
See helpers/cache/README.md for implementation details.

Import as:

import helpers.cache as hcache
"""

import copy
import functools
import logging
import os
import time
from typing import Any, Callable, Optional, Tuple, Union

import joblib
import joblib.func_inspect as jfunci
import joblib.memory as jmemor

import helpers.dbg as dbg
import helpers.git as git
import helpers.introspection as hintro
import helpers.io_ as hio
import helpers.system_interaction as hsyste

_LOG = logging.getLogger(__name__)

# Log level for information about the high level behavior of the caching layer.
_LOG_LEVEL = logging.DEBUG

# #############################################################################


def get_cache_types() -> List[str]:
    """
    Return the types (aka levels) of the cache.
    """
    return ["mem", "disk"]


def _check_valid_cache_type(cache_type: str) -> None:
    """
    Assert that `cache_type` is a valid cache type.
    """
    dbg.dassert_in(cache_type, get_cache_types())


# #############################################################################


_USE_CACHING: bool = True


def set_caching(val: bool) -> None:
    """
    Enable or disable cache for all usages.
    """
    global _USE_CACHING
    _LOG.warning("Setting caching to %s -> %s", _USE_CACHING, val)
    _USE_CACHING = val


def is_caching_enabled() -> bool:
    """
    Check if cache is enabled.

    :return: whether the cache is enabled or not
    """
    return _USE_CACHING


# #############################################################################


_MEMORY_TMPFS_PATH = os.getenv(
    "CACHE_MEMORY_TMPFS_PATH",
    "/tmp" if hsyste.get_os_name() == "Darwin" else "/mnt/tmpfs",
)


def get_cache_name(cache_type: str, tag: Optional[str] = None) -> str:
    """
    Get the canonical cache name (e.g., "tmp.cache.mem.tag") for a type of
    cache.

    :param cache_type: type of a cache
    :param tag: optional unique tag of the cache, empty by default
    :return: name of the folder for a cache
    """
    _check_valid_cache_type(cache_type)
    cache_name = "tmp.cache"
    cache_name += f".{cache_type}"
    if tag is not None:
        cache_name += f".{tag}"
    return cache_name


def get_cache_path(cache_type: str, tag: Optional[str] = None) -> str:
    """
    Get path to the directory storing the cache.

    For disk cache, the path is on the file system relative to Git root.
    For memory cache, the path is in a predefined RAM disk.

    :param cache_type: type of a cache
    :param tag: optional unique tag of the cache, empty by default
    :return: a file system path
    """
    _check_valid_cache_type(cache_type)
    cache_name = get_cache_name(cache_type, tag)
    if cache_type == "mem":
        root_path = _MEMORY_TMPFS_PATH
    elif cache_type == "disk":
        root_path = git.get_client_root(super_module=True)
    file_name = os.path.join(root_path, cache_name)
    file_name = os.path.abspath(file_name)
    return file_name


def _create_cache_backend(
    cache_type: str, tag: Optional[str] = None
) -> joblib.Memory:
    """
    Create an object storing a cache.

    :param cache_type: type of a cache
    :param tag: optional unique tag of the cache, empty by default
    :return: cache backend object
    """
    _check_valid_cache_type(cache_type)
    file_name = get_cache_path(cache_type, tag)
    cache_backend = joblib.Memory(file_name, verbose=0, compress=1)
    return cache_backend


# #############################################################################

# This is the global memory cache.
_MEMORY_CACHE: Any = None


# This is the global disk cache.
_DISK_CACHE: Any = None


def get_global_cache(cache_type: str, tag: Optional[str] = None) -> joblib.Memory:
    """
    Get global cache by cache type.

    :param cache_type: type of a cache
    :param tag: optional unique tag of the cache, empty by default
    :return: caching backend
    """
    _check_valid_cache_type(cache_type)
    global _MEMORY_CACHE
    global _DISK_CACHE
    if tag is None:
        if cache_type == "mem":
            if _MEMORY_CACHE is None:
                # Create global memory cache if it doesn't exist.
                _MEMORY_CACHE = _create_cache_backend(cache_type)
            global_cache = _MEMORY_CACHE
        elif cache_type == "disk":
            if _DISK_CACHE is None:
                # Create global disk cache if it doesn't exist.
                _DISK_CACHE = _create_cache_backend(cache_type)
            global_cache = _DISK_CACHE
    else:
        # Build a one-off cache using tag.
        global_cache = _create_cache_backend(cache_type, tag)
    return global_cache


def set_global_cache(cache_type: str, cache_backend: joblib.Memory) -> None:
    """
    Set global cache by cache type.

    :param cache_type: type of a cache
    :param cache_backend: caching backend
    """
    _check_valid_cache_type(cache_type)
    global _MEMORY_CACHE
    global _DISK_CACHE
    if cache_type == "mem":
        _MEMORY_CACHE = cache_backend
    elif cache_type == "disk":
        _DISK_CACHE = cache_backend


def clear_global_cache(cache_type: str, tag: Optional[str] = None) -> None:
    """
    Reset a cache by cache type.

    :param cache_type: type of a cache
    :param tag: optional unique tag of the cache, empty by default
    """
    _check_valid_cache_type(cache_type)
    _LOG.warning(
        "Resetting %s cache '%s'", cache_type, get_cache_path(cache_type, tag)
    )
    disk_cache = get_global_cache(cache_type, tag)
    disk_cache.clear(warn=True)


def destroy_global_cache(cache_type: str, tag: Optional[str] = None) -> None:
    """
    Destroy a cache by cache type and remove physical directory.

    :param cache_type: type of a cache
    :param tag: optional unique tag of the cache, empty by default
    """
    _check_valid_cache_type(cache_type)
    cache_path = get_cache_path(cache_type, tag)
    _LOG.warning("Destroying %s cache '%s'", cache_type, cache_path)
    hio.delete_dir(cache_path)


# #############################################################################


class Cached:
    # pylint: disable=protected-access
    """
    Decorator wrapping a function in a disk and memory cache.

    If the function value was not cached either in memory or on disk, the
    function `f()` is executed and the value is stored.

    The decorator uses 2 levels of caching:
    - disk cache: useful for retrieving the state among different executions or
      when one does a "Reset" of a notebook;
    - memory cache: useful for multiple execution in notebooks, without
      resetting the state.
    """

    def __init__(
        self,
        func: Callable,
        use_mem_cache: bool = True,
        use_disk_cache: bool = True,
        set_verbose_mode: bool = True,
        tag: Optional[str] = None,
        disk_cache_directory: Optional[str] = None,
        mem_cache_directory: Optional[str] = None,
    ):
        """
        :param set_verbose_mode: print high-level information about the cache behavior
            (e.g., whether a function was cached and from which level, execution time)
        """
        # This is used to make the class have the same attributes (e.g.,
        # `__name__`, `__doc__`, `__dict__`) as the called function.
        functools.update_wrapper(self, func)
        self._func = func
        self._use_mem_cache = use_mem_cache
        self._use_disk_cache = use_disk_cache
        self._set_verbose_mode = set_verbose_mode
        # Set value for disk cache directory.
        self._disk_cache_directory = disk_cache_directory
        # Set value for mem cache directory.
        self._mem_cache_directory = mem_cache_directory
        self._tag = tag
        self._reset_cache_tracing()
        # Create the disk and mem cache objects.
        self._create_cache("disk")
        self._create_cache("mem")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # TODO(gp): Use helpers/timer.
        perf_counter_start = 0.0
        if self._set_verbose_mode:
            perf_counter_start = time.perf_counter()
        # Execute the cached function.
        if not is_caching_enabled():
            _LOG.warning("Caching is disabled")
            self._last_used_disk_cache = self._last_used_mem_cache = False
            obj = self._func(*args, **kwargs)
        else:
            self._reset_cache_tracing()
            obj = self._execute_func(*args, **kwargs)
            _LOG.log(
                _LOG_LEVEL,
                "%s: executed from '%s'",
                self._func.__name__,
                self.get_last_cache_accessed(),
            )
            obj = copy.deepcopy(obj)
        # Print caching info.
        if self._set_verbose_mode:
            perf_counter = time.perf_counter() - perf_counter_start
            obj_size = hintro.get_size_in_bytes(obj)
            obj_size_as_str = hintro.format_size(obj_size)
            _LOG.info(
                "Data for '%s' (size=%s) was retrieved from %s in %f sec",
                self._func.__name__,
                obj_size_as_str,
                self.get_last_cache_accessed(),
                perf_counter,
            )
        return obj

    def get_last_cache_accessed(self) -> str:
        """
        Get the cache used in the latest call.

        :return: type of a cache used in the last call
        """
        if self._last_used_mem_cache:
            ret = "mem"
        elif self._last_used_disk_cache:
            ret = "disk"
        else:
            ret = "no_cache"
        return ret

    # ///////////////////////////////////////////////////////////////////////////
    # Function-specific cache.
    # ///////////////////////////////////////////////////////////////////////////

    def clear_cache(self, cache_type: Optional[str] = None) -> None:
        """
        Clear all caches or a cache by type. Only works in function-specific
        case.

        :param cache_type: type of a cache to clear, or `None` to clear all caches
        """
        if cache_type is None:
            # Clear both.
            self.clear_cache("mem")
            self.clear_cache("disk")
            # dbg.dassert_is_not(self._disk_cache_directory, None,
            #         "Cannot clear the global disk cache")
            # disk_cache = self._get_cache("disk")
            # disk_cache.clear()
            # #
            # dbg.dassert_is_not(self._mem_cache_directory, None,
            #                   "Cannot clear the global mem cache")
            # mem_cache = self._get_cache("mem")
            # mem_cache.clear()
        else:
            if cache_type == "mem":
                cache_path = self._mem_cache_directory
            elif cache_type == "disk":
                cache_path = self._disk_cache_directory
            if cache_path is None:
                dbg.dassert_is_not(
                    cache_path,
                    None,
                    "Cannot clear the global %s cache",
                    cache_type,
                )
            cache_backend = self._get_cache(cache_type)
            cache_backend.clear()

    def destroy_cache(self, cache_type: str) -> None:
        """
        Destroy a cache by cache type and remove physical directory. Only works
        in cache-specific case.

        :param cache_type: type of a cache
        """
        _check_valid_cache_type(cache_type)
        if cache_type == "mem":
            cache_path = self._mem_cache_directory
        else:
            cache_path = self._disk_cache_directory
        dbg.dassert_is_not(
            cache_path, None, "Cannot destroy global %s cache", cache_type
        )
        # Paranoia dfatal: maybe we can prompt the user to confirm.
        dbg.dfatal(
            "Remove this dfatal() to destroy %s cache '%s'"
            % (cache_type, cache_path)
        )
        hio.delete_dir(cache_path)

    def set_cache_directory(
        self, cache_type: str, cache_path: Optional[str]
    ) -> None:
        """
        Set the cache directory for a cache type.

        :param cache_type: type of a cache
        :param cache_path: cache directory or None for global cache
            - If `None` is passed then use global cache.
        """
        _check_valid_cache_type(cache_type)
        if cache_type == "mem":
            self._mem_cache_directory = cache_path
        else:
            self._disk_cache_directory = cache_path
        self._create_cache(cache_type)

    def get_cache_directory(self, cache_type: str) -> Optional[str]:
        """
        Get the cache directory for a cache type, `None` if global cache is
        used.

        :param cache_type: type of a cache
        :return: directory for specific cache or None if global cache is used
        """
        _check_valid_cache_type(cache_type)
        if cache_type == "mem":
            ret = self._mem_cache_directory
        elif cache_type == "disk":
            ret = self._disk_cache_directory
        return ret

    # ///////////////////////////////////////////////////////////////////////////

    def _create_cache(self, cache_type: str) -> None:
        """
        Return an object storing a cache.

        :param cache_type: type of a cache
        """
        _check_valid_cache_type(cache_type)
        if cache_type == "mem":
            if self._mem_cache_directory:
                self._memory_cache = joblib.Memory(
                    self._mem_cache_directory, verbose=0, compress=1
                )
            else:
                self._memory_cache = get_global_cache(cache_type, self._tag)
            self._memory_cached_func = self._memory_cache.cache(self._func)
        else:
            if self._disk_cache_directory:
                self._disk_cache = joblib.Memory(
                    self._disk_cache_directory, verbose=0, compress=1
                )
            else:
                self._disk_cache = get_global_cache(cache_type, self._tag)
            self._disk_cached_func = self._disk_cache.cache(self._func)

    def _get_identifiers(
        self, cache_type: str, args: Any, kwargs: Any
    ) -> Tuple[str, str]:
        """
        Get digests for current function and arguments to be used in cache.

        :param cache_type: type of a cache
        :param args: original arguments of the call
        :param kwargs: original kw-arguments of the call
        :return: digests of the function and current arguments
        """
        cache_backend = self._get_cache(cache_type)
        func_id, args_id = cache_backend._get_output_identifiers(*args, **kwargs)
        return func_id, args_id

    def _get_cache(self, cache_type: str) -> joblib.MemorizedResult:
        """
        Get the instance of a cache by type.

        :param cache_type: type of a cache
        :return: instance of the cache from joblib
        """
        _check_valid_cache_type(cache_type)
        if cache_type == "mem":
            cache_backend = self._memory_cached_func
        elif cache_type == "disk":
            cache_backend = self._disk_cached_func
        return cache_backend

    def _has_cached_version(
        self, cache_type: str, func_id: str, args_id: str
    ) -> bool:
        """
        Check if a cache contains an entry for a corresponding function and
        arguments digests, and that function source has not changed.

        :param cache_type: type of a cache
        :param func_id: digest of the function obtained from _get_identifiers
        :param args_id: digest of arguments obtained from _get_identifiers
        :return: whether there is an entry in a cache
        """
        cache_backend = self._get_cache(cache_type)
        has_cached_version = cache_backend.store_backend.contains_item(
            [func_id, args_id]
        )
        if has_cached_version:
            # We must check that the source of the function is the same.
            # Otherwise, cache tracing will not be correct.
            # First, try faster check via joblib hash.
            if self._func in jmemor._FUNCTION_HASHES:
                func_hash = cache_backend._hash_func()
                if func_hash == jmemor._FUNCTION_HASHES[self._func]:
                    return True
            # Otherwise, check the the source of the function is still the same.
            func_code, _, _ = jmemor.get_func_code(self._func)
            old_func_code_cache = (
                cache_backend.store_backend.get_cached_func_code([func_id])
            )
            old_func_code, _ = jmemor.extract_first_line(old_func_code_cache)
            if func_code == old_func_code:
                return True
        return False

    def _store_cached_version(
        self, cache_type: str, func_id: str, args_id: str, obj: Any
    ) -> None:
        """
        Store returned value from the intrinsic function in the cache.

        :param cache_type: type of a cache
        :param func_id: digest of the function obtained from _get_identifiers
        :param args_id: digest of arguments obtained from _get_identifiers
        :param obj: return value of the intrinsic function
        """
        cache_backend = self._get_cache(cache_type)
        # Write out function code to the cache.
        func_code, _, first_line = jfunci.get_func_code(cache_backend.func)
        cache_backend._write_func_code(func_code, first_line)
        # Store the returned value into the cache.
        cache_backend.store_backend.dump_item([func_id, args_id], obj)

    # ///////////////////////////////////////////////////////////////////////////

    def _reset_cache_tracing(self) -> None:
        """
        Reset the values used to track which cache we are hitting when
        executing the cached function.
        """
        self._last_used_disk_cache = self._use_disk_cache
        self._last_used_mem_cache = self._use_mem_cache

    def _execute_func_from_disk_cache(self, *args: Any, **kwargs: Any) -> Any:
        func_id, args_id = self._get_identifiers("disk", args, kwargs)
        if not self._has_cached_version("disk", func_id, args_id):
            # If we get here, we didn't hit neither memory nor the disk cache.
            self._last_used_disk_cache = False
            _LOG.debug(
                "%s(args=%s kwargs=%s): execute the intrinsic function",
                self._func.__name__,
                args,
                kwargs,
            )
        obj = self._disk_cached_func(*args, **kwargs)
        return obj

    def _execute_func_from_mem_cache(self, *args: Any, **kwargs: Any) -> Any:
        func_id, args_id = self._get_identifiers("mem", args, kwargs)
        _LOG.debug(
            "%s: use_mem_cache=%s use_disk_cache=%s",
            self._func.__name__,
            self._use_mem_cache,
            self._use_disk_cache,
        )
        if self._has_cached_version("mem", func_id, args_id):
            obj = self._memory_cached_func(*args, **kwargs)
        else:
            # If we get here, we know that we didn't hit the memory cache,
            # but we don't know about the disk cache.
            self._last_used_mem_cache = False
            #
            if self._use_disk_cache:
                _LOG.debug(
                    "%s(args=%s kwargs=%s): trying to read from disk",
                    self._func.__name__,
                    args,
                    kwargs,
                )
                obj = self._execute_func_from_disk_cache(*args, **kwargs)
            else:
                _LOG.warning("Skipping disk cache")
                obj = self._memory_cached_func(*args, **kwargs)
            self._store_cached_version("mem", func_id, args_id, obj)
        return obj

    def _execute_func(self, *args: Any, **kwargs: Any) -> Any:
        _LOG.debug(
            "%s: use_mem_cache=%s use_disk_cache=%s",
            self._func.__name__,
            self._use_mem_cache,
            self._use_disk_cache,
        )
        if self._use_mem_cache:
            _LOG.debug(
                "%s(args=%s kwargs=%s): trying to read from memory",
                self._func.__name__,
                args,
                kwargs,
            )
            obj = self._execute_func_from_mem_cache(*args, **kwargs)
        else:
            _LOG.warning("Skipping memory cache")
            if self._use_disk_cache:
                obj = self._execute_func_from_disk_cache(*args, **kwargs)
            else:
                _LOG.warning("Skipping disk cache")
                obj = self._func(*args, **kwargs)
        return obj


# #############################################################################
# Decorator
# #############################################################################


def cache(
    fn: Optional[Callable] = None,
    use_mem_cache: bool = True,
    use_disk_cache: bool = True,
    set_verbose_mode: bool = False,
    tag: Optional[str] = None,
    disk_cache_directory: Optional[str] = None,
    mem_cache_directory: Optional[str] = None,
) -> Union[Callable, Cached]:
    """
    Decorate.

    Usage examples:

    import helpers.cache as hcache

    @hcache.cache
    def add(x: int, y: int) -> int:
        return x + y

    @hcache.cache(use_mem_cache=False)
    def add(x: int, y: int) -> int:
        return x + y

    :param fn: function to decorate with Cached class
    :param use_mem_cache: whether to use memory cache
    :param use_disk_cache: whether to use disk cache
    :param set_verbose_mode: whether to report performance metrics
    :param tag: optional tag to separate cache from the global one, if set
    :param mem_cache_directory: optional path to a specific directory to store mem cache.
    :param disk_cache_directory: optional path to a specific directory to store disk cache.
    :return: Cached instance if fn is set, otherwise a function decorator
    """
    if callable(fn):
        return Cached(
            fn,
            use_mem_cache=use_mem_cache,
            use_disk_cache=use_disk_cache,
            set_verbose_mode=set_verbose_mode,
            disk_cache_directory=disk_cache_directory,
            mem_cache_directory=mem_cache_directory,
            tag=tag,
        )

    def wrapper(func: Callable) -> Cached:
        return Cached(
            func,
            use_mem_cache=use_mem_cache,
            use_disk_cache=use_disk_cache,
            set_verbose_mode=set_verbose_mode,
            disk_cache_directory=disk_cache_directory,
            mem_cache_directory=mem_cache_directory,
            tag=tag,
        )

    return wrapper
