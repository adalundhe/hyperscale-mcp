
import asyncio
import json
import psutil
import functools
from typing import Literal
from mcp.server.fastmcp import FastMCP, Context
from .tasks import TaskRunner, Env

mcp = FastMCP("hyperscale")
runner = TaskRunner(0, Env())


@mcp.tool()
async def run_test(
    test: str, 
    config: str | None = None,
    log_level: Literal[
        'trace',
        'debug',
        'info',
        'warn',
        'error',
        'critical',
        'fatal',
    ] = 'error',
    name: str = 'default',
    workers: int | None = None,
    timeout: str | None = None,
):
    '''
    Run the specified test at the given path.

    Args:
        test: Path to a *.py file containing the test (e.g. test.py, example.py)
        config: Optional path to a *.json file containing a JSON config (e.g. .hyperscale.json, config.json)
        log_level: A valid log level. Must be one of trace, debug, info, warn, error, critical, or fatal. Default is error.
        name: Optional name of the test (e.g. example, cicd, check_api). Default is default.
        quiet: If true, disables the stdout output
        workers: Optional maximum number of CPU cores to use for the test. Default is None.
        timeout: Optional timeout for the test. Default is None. If not specified, keep polling until result is returned.

    Returns:
        A JSON string of containing the task_name and run_id to use in getting the command output.
    '''
    args = [
        'run',
        test,
        '--name',
        name,
        '-q'
    ]

    if workers is None:
        loop = asyncio.get_event_loop()
        workers = await loop.run_in_executor(
            None,
            functools.partial(
                psutil.cpu_count,
                logical=False,
            )
        )

        workers -= 1
    
    if log_level:
        args.extend(['-l', log_level])

    if workers:
        args.extend(['-w', str(workers)])
    
    if config:
        args.extend(['-c', config])

    run = runner.command(
        'hyperscale',
        *args,
        alias='run_test',
        timeout=timeout,
    )


    return json.dumps({
        'task_name': 'run_test',
        'run_id': run.run_id
    })

@mcp.tool()
async def new_test(
    test: str,
    overwrite: bool = False,
    timeout: str | None = None,
):
    '''
    Create a new Hyperscale test at the specified path.

    Args:
        path: The path at which to create the test (e.g. test.py, example.py). If any directories in the subpath do not exist, they should be created first.
        overwrite: If true, allow Hyperscale to overwrite any pre-existing file at the path.
        timeout: Optional timeout to create a new test. Default is None. If not specified, keep polling until result is returned and do not restart or cancel the test.

    Returns:
        A JSON string of containing the task_name and run_id to use in getting the command output.
    '''

    args = [
        'new',
        test,
    ]

    if overwrite:
        args.append('-o')

    run = runner.command(
        'hyperscale',
        *args,
        alias='new_test',
        timeout=timeout,
    )

    return json.dumps({
        'task_name': 'new_test',
        'run_id': run.run_id
    })

@mcp.tool()
async def get_command_output(
    ctx: Context,
    task_name: str,
    run_id: str | int,
) -> str:
    """Get the output of an executed Hyperscale command. The run_id should be the run_id
    returned by the most recent Hyperscale-MCP new_test or run_test response and the task_name
    should be the name of the most recent Hyperscale-MCP tool call.
    
    Args:
        ctx: MCP context for providing progress updates
        task_name: Name of the task that we are checking.
        run_id: ID of the task run we are checking. Should be the run_id of the most recently run command.

    Returns:
        A JSON string of the current task run output.
        If the command is still running, the function will return a status of "RUNNING".

    Examples:
        "Show me the output of my latest hyperscale command"
        "What is the output of run 1234567890?"
        "What is the status of the current test run?"
        "Has the new test been created?"
    """

    if isinstance(run_id, str):
        run_id = int(run_id)
    
    ctx.info(f"Getting output of task {task_name} run {run_id}...")

    result = await runner.get_task_update(task_name, run_id)

    return result.model_dump_json()


def run():
    """Entry point for the Hyperscale MCP server"""
    mcp.run()
    