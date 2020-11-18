import json
import logging
import os
import os.path
import re
import sys
from typing import Optional

import ipykernel
import requests

# Alternative that works for both Python 2 and 3:
import requests.compat as reqc

import helpers.telegram_notify.config as tgcfg

try:  # Python 3 (see Edit2 below for why this may not work in Python 2)
    import notebook.notebookapp as ihnb
except ImportError:  # Python 2
    import warnings

    import IPython.utils.shimmodule as iush

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=iush.ShimWarning)
        import IPython.html.notebookapp as ihnb

_LOG = logging.getLogger(__name__)


def _get_launcher_name() -> str:
    """Return the name of jupyter notebook or path to python file you are
    running."""
    launcher = sys.argv[0]
    if os.path.basename(launcher) == "ipykernel_launcher.py":
        match = re.search(
            "kernel-(.*).json", ipykernel.connect.get_connection_file()
        )
        if match is None:
            return launcher
        kernel_id = match.group(1)
        servers = ihnb.list_running_servers()
        for ss in servers:
            response = requests.get(
                reqc.urljoin(ss["url"], "api/sessions"),
                params={"token": ss.get("token", "")},
            )
            for nn in json.loads(response.text):
                if nn["kernel"]["id"] == kernel_id:
                    relative_path = nn["notebook"]["path"]
                    return str(os.path.basename(relative_path))
    return launcher


# #############################################################################
# Send message
# #############################################################################


class TelegramNotify:
    """Sends notifications."""

    def __init__(self) -> None:
        self.launcher_name = _get_launcher_name()
        self.token, self.chat_id = tgcfg.get_info()

    def notify(self, message: str) -> None:
        msg = "<pre>{notebook_name}</pre>: {message}".format(
            notebook_name=self.launcher_name, message=message
        )
        self._send(msg, self.token, self.chat_id)

    @staticmethod
    def _send(
        text: str, token: Optional[str], chat_id: Optional[str]
    ) -> Optional[bytes]:
        if chat_id is None or token is None:
            _LOG.warning(
                "Not sending notifications. To send notifications, both "
                "`chat_id` and `token` need to be specified. Go to README.md"
                "for more information."
            )
            return None
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        return requests.post(
            "https://api.telegram.org/bot{token}/sendMessage".format(token=token),
            data=payload,
        ).content


# #############################################################################
# Send notifications using logging
# #############################################################################


class _RequestsHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> bytes:
        token, chat_id = tgcfg.get_info()
        log_entry = self.format(record)
        payload = {"chat_id": chat_id, "text": log_entry, "parse_mode": "HTML"}
        return requests.post(
            "https://api.telegram.org/bot{token}/sendMessage".format(token=token),
            data=payload,
        ).content


class _LOGstashFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        launcher_name = _get_launcher_name()
        return "<pre>{notebook_name}</pre>: {message}".format(
            message=record.msg, notebook_name=launcher_name
        )


def init_tglogger(log_level: int = logging.DEBUG) -> None:
    _tg_log = logging.getLogger("telegram_notify")
    _tg_log.setLevel(log_level)
    handler = _RequestsHandler()
    formatter = _LOGstashFormatter()
    handler.setFormatter(formatter)
    _tg_log.handlers = [handler]
