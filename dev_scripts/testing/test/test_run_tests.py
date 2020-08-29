import logging
import os
from typing import List, Tuple

import pytest

import dev_scripts.linter2 as lntr
import dev_scripts.url as url
import helpers.conda as hco
import helpers.dbg as dbg
import helpers.env as env
import helpers.git as git
import helpers.io_ as io_
import helpers.system_interaction as si
import helpers.unit_test as ut

_LOG = logging.getLogger(__name__)


class Test_url_py1(ut.TestCase):
    def test_get_file_name1(self) -> None:
