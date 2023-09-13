# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/004_Test_Utils.ipynb.

# %% auto 0
__all__ = ['logger', 'nb_safe_seed', 'mock_AIOKafkaProducer_send', 'run_script_and_cancel', 'display_docs']

# %% ../../nbs/004_Test_Utils.ipynb 1
import asyncio
import hashlib
import platform
import shlex
import signal
import subprocess  # nosec
import unittest
import unittest.mock
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import *

import asyncer
from IPython.display import IFrame

from .._application.app import FastKafka
from .._components._subprocess import terminate_asyncio_process
from .._components.helpers import _import_from_string, change_dir
from .._components.logger import get_logger

# %% ../../nbs/004_Test_Utils.ipynb 4
logger = get_logger(__name__)

# %% ../../nbs/004_Test_Utils.ipynb 6
def nb_safe_seed(s: str) -> Callable[[int], int]:
    """Gets a unique seed function for a notebook

    Params:
        s: name of the notebook used to initialize the seed function

    Returns:
        A unique seed function
    """
    init_seed = int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % (10**8)

    def _get_seed(x: int = 0, *, init_seed: int = init_seed) -> int:
        return init_seed + x

    return _get_seed

# %% ../../nbs/004_Test_Utils.ipynb 8
@contextmanager
def mock_AIOKafkaProducer_send() -> Generator[unittest.mock.Mock, None, None]:
    """Mocks **send** method of **AIOKafkaProducer**"""
    with unittest.mock.patch("__main__.AIOKafkaProducer.send") as mock:

        async def _f() -> None:
            pass

        mock.return_value = asyncio.create_task(_f())

        yield mock

# %% ../../nbs/004_Test_Utils.ipynb 9
async def run_script_and_cancel(
    script: str,
    *,
    script_file: Optional[str] = None,
    cmd: Optional[str] = None,
    cancel_after: int = 10,
    app_name: str = "app",
    kafka_app_name: str = "kafka_app",
    generate_docs: bool = False,
) -> Tuple[int, bytes]:
    """
    Runs a script and cancels it after a predefined time.

    Args:
        script: A python source code to be executed in a separate subprocess.
        script_file: Name of the script where script source will be saved.
        cmd: Command to execute. If None, it will be set to 'python3 -m {Path(script_file).stem}'.
        cancel_after: Number of seconds before sending SIGTERM signal.
        app_name: Name of the app.
        kafka_app_name: Name of the Kafka app.
        generate_docs: Flag indicating whether to generate docs.

    Returns:
        A tuple containing the exit code and combined stdout and stderr as a binary string.
    """
    if script_file is None:
        script_file = "script.py"

    if cmd is None:
        cmd = f"python3 -m {Path(script_file).stem}"

    with TemporaryDirectory() as d:
        consumer_script = Path(d) / script_file

        with open(consumer_script, "w") as file:
            file.write(script)

        if generate_docs:
            logger.info(
                f"Generating docs for: {Path(script_file).stem}:{kafka_app_name}"
            )
            try:
                kafka_app: FastKafka = _import_from_string(
                    f"{Path(script_file).stem}:{kafka_app_name}"
                )
                await asyncer.asyncify(kafka_app.create_docs)()
            except Exception as e:
                logger.warning(
                    f"Generating docs failed for: {Path(script_file).stem}:{kafka_app_name}, ignoring it for now."
                )

        creationflags = 0 if platform.system() != "Windows" else subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore
        proc = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=d,
            shell=True  # nosec: [B602:subprocess_without_shell_equals_true] subprocess call - check for execution of untrusted input.
            if platform.system() == "Windows"
            else False,
            creationflags=creationflags,
        )
        await asyncio.sleep(cancel_after)
        if platform.system() == "Windows":
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore
        else:
            proc.terminate()
        output, _ = proc.communicate()

        return (proc.returncode, output)

# %% ../../nbs/004_Test_Utils.ipynb 14
async def display_docs(docs_path: str, port: int = 4000) -> None:
    """
    Serves the documentation using an HTTP server.

    Args:
        docs_path: Path to the documentation.
        port: Port number for the HTTP server. Defaults to 4000.

    Returns:
        None
    """
    with change_dir(docs_path):
        process = await asyncio.create_subprocess_exec(
            "python3",
            "-m",
            "http.server",
            f"{port}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            from google.colab.output import eval_js

            proxy = eval_js(f"google.colab.kernel.proxyPort({port})")
            logger.info("Google colab detected! Proxy adjusted.")
        except:
            proxy = f"http://localhost:{port}"
        finally:
            await asyncio.sleep(2)
            display(IFrame(f"{proxy}", 1000, 700))  # type: ignore
            await asyncio.sleep(2)
            await terminate_asyncio_process(process)
