import asyncio
import signal
import shlex
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from typing import (
    Any,
    Dict,
    Optional,
    TypeVar,
    Literal,
    Callable,
    Awaitable,
)

from .env import Env
from .models import RunStatus, TaskType
from .snowflake_generator import SnowflakeGenerator
from .time_parser import TimeParser

from .task_hook import Task

T = TypeVar("T")

def shutdown_executor(
    sig: int,
    executor: ThreadPoolExecutor | ProcessPoolExecutor, 
    default_handler: Callable[..., Any]
):
    executor.shutdown(cancel_futures=True)
    signal.signal(sig, default_handler)


class TaskRunner:
    def __init__(self, instance_id: int, config: Env) -> None:
        self.tasks: Dict[str, Task[Any]] = {}
        self.results: Dict[str, Any]
        self._cleanup_interval = TimeParser(config.MERCURY_SYNC_CLEANUP_INTERVAL).time
        self._cleanup_task: Optional[asyncio.Task] = None
        self._run_cleanup: bool = False
        self._snowflake_generator = SnowflakeGenerator(instance_id)

        if config.MERCURY_SYNC_EXECUTOR_TYPE == 'thread':
            self._executor = ThreadPoolExecutor(max_workers=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS)

        else:

            self._executor = ProcessPoolExecutor(max_workers=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS)

        self._executor_sempahore = asyncio.Semaphore(value=config.MERCURY_SYNC_TASK_RUNNER_MAX_THREADS)
        self._loop = asyncio.get_event_loop()

        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIG_IGN]:

            default_handler = signal.getsignal(sig)

            self._loop.add_signal_handler(
                sig,
                lambda: shutdown_executor(
                    sig,
                    self._executor, 
                    default_handler,
                )
            )

    def all_tasks(self):
        for task in self.tasks.values():
            yield task

    def start_cleanup(self):
        self._run_cleanup = True
        self._cleanup_task = asyncio.ensure_future(self._cleanup())

    def create_task_id(self):
        return self._snowflake_generator.generate()

    def run(
        self,
        call: Callable[..., Awaitable[Any]],
        *args,
        run_id: int | None = None,
        timeout: int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = 'MANUAL',
        repeat: Literal["NEVER", "ALWAYS"] | int = 'NEVER',
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = 'COUNT',
        **kwargs,
    ):
        
        if self._cleanup_task is None:
            self.start_cleanup()

        command_name = call.__name__
        task = self.tasks.get(command_name)
        if task is None and call:
            task = Task(
                self._snowflake_generator,
                command_name,
                call,
                self._executor,
                self._executor_sempahore,
                schedule=schedule,
                trigger=trigger,
                repeat=repeat,
                keep=keep,
                max_age=max_age,
                keep_policy=keep_policy,
            )

            self.tasks[command_name] = task

        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time

        if task and task.repeat == "NEVER":
            return task.run(
                *args,
                **kwargs,
                run_id=run_id,
                timeout=timeout,
            )

        elif task and task.schedule:
            return task.run_schedule(
                *args,
                **kwargs,
                run_id=run_id,
                timeout=timeout,
            )
        
    def command(
        self,
        command: str,
        *args: tuple[str, ...],
        alias: str | None = None,
        env: dict[str, Any] | None = None,
        cwd: str | None = None,
        shell: bool = False,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = 'MANUAL',
        repeat: Literal["NEVER", "ALWAYS"] | int = 'NEVER',
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = 'COUNT',
    ):
        if self._cleanup_task is None:
            self.start_cleanup()

        command_name = alias
        if command_name is None:
            command_name = command

        if isinstance(timeout, str):
            timeout = TimeParser(timeout).time

        if shell:
            args = [shlex.quote(arg) for arg in args]

        task = self.tasks.get(command_name)
        if task is None:
            task = Task(
                self._snowflake_generator,
                command_name,
                command,
                self._executor,
                self._executor_sempahore,
                schedule=schedule,
                trigger=trigger,
                repeat=repeat,
                keep=keep,
                max_age=max_age,
                keep_policy=keep_policy,
                task_type=TaskType.SHELL,
            )

            self.tasks[command_name] = task

        if task and task.repeat == "NEVER":
            return task.run_shell(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                run_id=run_id,
                timeout=timeout,
                poll_interval=self._cleanup_interval,
            )

        elif task and task.schedule:
            return task.run_shell_schedule(
                *args,
                env=env,
                cwd=cwd,
                shell=shell,
                run_id=run_id,
                timeout=timeout,
            )
        
    async def wait(
        self,
        command_name: str,
        run_id: int,
    ):
        update = await self.get_task_update(command_name, run_id)
        while update.status not in [RunStatus.COMPLETE, RunStatus.FAILED, RunStatus.CANCELLED]:
            await asyncio.sleep(self._cleanup_interval)
            update = await self.get_task_update(command_name, run_id)

        return await self.tasks[command_name].complete(run_id)
        
    async def get_task_update(
        self,
        command_name: str,
        run_id: int,
    ):
        return await self.tasks[command_name].get_run_update(run_id)
    

    def stop(
        self,
        task_name: str,
    ):
        task = self.tasks.get(task_name)
        if task:
            task.stop()

    def get_task_status(self, task_name: str):
        if task := self.tasks.get(task_name):
            return task.status

    def get_run_status(self, task_name: str, run_id: str):
        if task := self.tasks.get(task_name):
            return task.get_run_status(run_id)

    async def complete(self, task_name: str, run_id: str):
        if task := self.tasks.get(task_name):
            return await task.complete(run_id)

    async def cancel(self, task_name: str, run_id: str):
        task = self.tasks.get(task_name)
        if task:
            await task.cancel(run_id)

    async def cancel_schedule(
        self,
        task_name: str,
    ):
        task = self.tasks.get(task_name)
        if task:
            await task.cancel_schedule()

    async def shutdown(self):
        for task in self.tasks.values():
            await task.shutdown()

        self._run_cleanup = False

        try:
            self._cleanup_task.cancel()
            await asyncio.sleep(0)
        
        except Exception:
            pass

        try:
            self._executor.shutdown(cancel_futures=True)

        except Exception:
            pass

    def abort(self):
        for task in self.tasks.values():
            task.abort()

        self._run_cleanup = False

        try:
            self._cleanup_task.set_result(None)

        except Exception:
            pass

        try:
            self._executor.shutdown(cancel_futures=True)

        except Exception:
            pass

    async def _cleanup(self):
        while self._run_cleanup:
            await self._cleanup_scheduled_tasks()
            await asyncio.sleep(self._cleanup_interval)

    async def _cleanup_scheduled_tasks(self):
        try:
            for task in self.tasks.values():
                await task.cleanup()

        except Exception:
            pass
