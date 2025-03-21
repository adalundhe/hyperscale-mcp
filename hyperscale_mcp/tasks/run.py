import asyncio
import functools
import inspect
import pathlib
import time
import traceback
from asyncio.subprocess import Process
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Awaitable, Callable, Optional, Dict

import psutil
from .models import (
    RunStatus,
    TaskRun,
    ShellProcess,
    CommandType,
    TaskType,
)


class Run:
    __slots__ = (
        "run_id",
        "status",
        "error",
        "trace",
        "start",
        "end",
        "elapsed",
        "timeout",
        "call",
        "result",
        "task_type",
        "_task",
        "_process",
        "_args",
        "_env",
        "_working_directory",
        "_command_type",
        "_buffer_size",
        '_read_lock',
        '_read_timeout',
        "_loop",
        "_executor",
        "_semaphore",
    )

    def __init__(
        self,
        run_id: int,
        call: Callable[..., Awaitable[Any]] | str,
        task_type: TaskType,
        executor: ProcessPoolExecutor | ThreadPoolExecutor,
        semaphore: asyncio.Semaphore,
        timeout: Optional[int] = None,
    ) -> None:
        self.run_id = run_id
        self.status = RunStatus.CREATED

        self._args: tuple[Any, ...] | None = None
        self._env: dict[str, Any] | None = None
        self._working_directory: str | None = None


        self.error: Optional[str] = None
        self.trace: Optional[str] = None
        self.start = time.monotonic()
        self.end = 0
        self.elapsed = 0
        self.timeout = timeout


        self.call = call
        self.task_type = task_type
        self.result: Any | None = None
        self._args: tuple[Any, ...] | None = None
        self._env: dict[str, Any] | None = None
        self._working_directory: str | None = None
        self._read_lock = asyncio.Lock()
        
        if not isinstance(self.call, str) and hasattr(call, '__self__'):
            bound_instance = call.__self__
            self.call = self.call.__get__(bound_instance, self.call.__class__)
            setattr(bound_instance, self.call.__name__, self.call)

        self._task: Optional[asyncio.Task] = None
        self._process: Process | None = None
        self._command_type: CommandType = 'subprocess'
        self._buffer_size = 8192
        self._read_timeout: int | float = 1
        self._loop = asyncio.get_event_loop()
        self._executor = executor
        self._semaphore = semaphore

    @property
    def running(self):
        return self.status == RunStatus.RUNNING

    @property
    def cancelled(self):
        return self.status == RunStatus.CANCELLED

    @property
    def failed(self):
        return self.status == RunStatus.FAILED

    @property
    def pending(self):
        return self.status == RunStatus.PENDING

    @property
    def completed(self):
        return self.status == RunStatus.COMPLETE

    @property
    def created(self):
        return self.status == RunStatus.CREATED
    
    @property
    def pid(self):
        if self._process:
            return self._process.pid
        
    @property
    def return_code(self):
        if self._process:
            return self._process.returncode
        
    async def get_stdout(self):
        buffer = bytearray()

        if self._process:
            chunk = await self._read_stdout_with_timeout()
            buffer.extend(chunk)

            while chunk:
                chunk = await self._read_stdout_with_timeout()
                buffer.extend(chunk)

        return bytes(buffer).decode()

    async def get_stderr(self):
        buffer = bytearray()
        if self._process:
            chunk = await self._read_stderr_with_timeout()
            buffer.extend(chunk)

            while chunk:
                chunk = await self._read_stderr_with_timeout()
                buffer.extend(chunk)

        return bytes(buffer).decode()

    async def _read_stderr_with_timeout(self):

        await self._read_lock.acquire()
        
        try:

            chunk = await asyncio.wait_for(
                self._process.stderr.read(self._buffer_size),
                timeout=self._read_timeout,
            )
        
        except asyncio.TimeoutError:
            chunk = b''

        if self._read_lock.locked():
            self._read_lock.release()

        return chunk

    async def _read_stdout_with_timeout(self):

        await self._read_lock.acquire()

        try:
            chunk = await asyncio.wait_for(
                self._process.stdout.read(self._buffer_size),
                timeout=self._read_timeout,
            )
        
        except asyncio.TimeoutError:
            chunk = b''
        
        if self._read_lock.locked():
            self._read_lock.release()

        return chunk
    
    @property
    def task_running(self):
        if self._process:
            return self._process.returncode is None

        return self._task and not self._task.done() and not self._task.cancelled()

    async def get_run_update(self):
        if self._process:
            stderr = await self.get_stderr()
            stdout = await self.get_stdout()

            return ShellProcess(
                    run_id=self.run_id,
                    process_id=self._process.pid,
                    command=self.call,
                    args=self._args,
                    status=self.status,
                    return_code=self._process.returncode,
                    env=self._env,
                    working_directory=self._working_directory,
                    command_type=self._command_type,
                    error=stderr,
                    result=stdout,
                    trace=self.trace,
                    elapsed=time.monotonic() - self.start
                )


        return TaskRun(
            run_id=self.run_id,
            status=self.status,
            error=self.error,
            trace=self.trace,
            start=self.start,
            end=self.end,
            elapsed=time.monotonic() - self.start,
            result=self.result,
        )

    def update_status(self, status: RunStatus):
        self.status = status
        self.elapsed = time.monotonic() - self.start

    async def complete(self):
        completed = self.status in [RunStatus.COMPLETE, RunStatus.FAILED]

        if completed:
            try:
                return await self._task

            except (asyncio.InvalidStateError, asyncio.CancelledError):
                pass

    async def cancel(self):
        if self._process:
            
            try:
                proc = psutil.Process(self._process.pid)
                proc.terminate()

            except Exception:
                pass

        try:
            self._task.set_result(None)

        except Exception:
            pass

        self.status = RunStatus.CANCELLED

    def abort(self):
        if self._process:
            self._process.kill()
            
        try:
            self._task.set_result(None)

        except Exception:
            pass

        self.status = RunStatus.CANCELLED

    def execute(self, *args, **kwargs):
        self._task = asyncio.ensure_future(self._execute(*args, **kwargs))

    def execute_shell(
        self,
        *args: tuple[Any, ...],
        poll_interval: int | float = 0.5,
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        timeout: int | float | None = None
    ):
        self._args = args
        self._env = env
        
        if cwd:
            self._working_directory = str(cwd)

        self._task = asyncio.ensure_future(self._execute_shell(
            *args,
            env=env,
            cwd=cwd,
            shell=shell,
            timeout=timeout,
            poll_interval=poll_interval,
        ))
        
    async def _execute_shell(
        self,
        *args: tuple[Any, ...],
        poll_interval: int | float = 0.5,
        env: Dict[str, str] | None = None,
        cwd: str | pathlib.Path | None = None,
        shell: bool = False,
        timeout: int | float | None = None,
    ):
        if shell:
            self._command_type = 'shell'

        working_directory: pathlib.Path | None = None
        if cwd:
            working_directory = pathlib.Path(cwd)

        try:

            if shell:
                command = [self.call]
                command.extend(args)

                self._process = await asyncio.create_subprocess_shell(
                    ' '.join(command),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=working_directory if cwd else None,
                )

            else:
                self._process = await asyncio.create_subprocess_exec(
                    self.call,
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=working_directory if cwd else None
                )

        except Exception:
            pass
        
        self.status = RunStatus.RUNNING

        try:
            if timeout:
                update =await asyncio.wait_for(
                    self._poll_for_shell_complete(poll_interval),
                    timeout=timeout,
                )

            else:
                update = await self._poll_for_shell_complete(poll_interval)

        except asyncio.TimeoutError:
            error = f"Err. - Task Run - {self.run_id} - timed out. Exceeded deadline of - {self.timeout} - seconds."
            self.status = RunStatus.FAILED

            update = ShellProcess(
                run_id=self.run_id,
                process_id=self._process.pid,
                command=self.call,
                args=self._args,
                status=self.status,
                return_code=self._process.returncode,
                env=self._env,
                working_directory=self._working_directory,
                command_type=self._command_type,
                error=error,
                trace=self.trace,
                elapsed=self.start - time.monotonic()

            )

        error_message = ''
        try:
            error_message = update.error

        except Exception:
            error_message = 'Unknown exception - failed to decode stderr output'
        
        self.error = error_message

        if self.return_code != 0:
            self.error = f"Err. - Task Run - {self.run_id} - failed. Encountered exception - {error_message}."
            self.status = RunStatus.FAILED

        else:
            self.status = RunStatus.COMPLETE
        try:
            self.result = update.result.decode()

        except Exception:
            pass

        return update

    async def _poll_for_shell_complete(
        self,
        poll_interval: int | float
    ):
        result = await self.get_run_update()
        while self._process.returncode is None:
            await asyncio.sleep(poll_interval)
            result = await self.get_run_update()

            self.elapsed = time.monotonic() - self.start
            

        return result

    async def _execute(self, *args, **kwargs):
        try:
            self.status = RunStatus.RUNNING

            is_coroutine = (
                inspect.iscoroutine(self.call)
                or inspect.isawaitable(self.call)
                or inspect.iscoroutinefunction(self.call)
            )
            
            if self.timeout and is_coroutine:

                self.result = await asyncio.wait_for(
                    self.call(*args, **kwargs), timeout=self.timeout
                )

            elif is_coroutine:
                self.result = await self.call(*args, **kwargs)

            elif self.timeout:
                await self._semaphore.acquire()
                self.result = await asyncio.wait_for(
                    self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.call,
                            *args,
                            **kwargs
                        )
                    )
                )

                self._semaphore.release()

            else:
                await self._semaphore.acquire()
                self.result = await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.call,
                        *args,
                        **kwargs
                    )
                )

                self._semaphore.release()
                
            self.status = RunStatus.COMPLETE

        except asyncio.TimeoutError:
            self.error = f"Err. - Task Run - {self.run_id} - timed out. Exceeded deadline of - {self.timeout} - seconds."
            self.status = RunStatus.FAILED

        except Exception as e:
            self.error = f"Err. - Task Run - {self.run_id} - failed. Encountered exception - {str(e)}."
            self.trace = traceback.format_exc()
            self.status = RunStatus.FAILED

        self.end = time.monotonic()
        self.elapsed = self.end - self.start

        return self.result
