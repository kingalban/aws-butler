from __future__ import annotations

import pytest
from types import SimpleNamespace
import botocore.session
from botocore.stub import Stubber
from unittest import mock
from click.testing import CliRunner


from parameters import (
    is_valid_ssm_name,
)

VALID, INVALID = True, False


@pytest.mark.parametrize(
    "name,validity", [
        ("/Dev/Production/East/Project-ABC/MyParameter", VALID),
        ("arn:aws:ssm:us-east-2:111122223333:parameter/param", VALID),   # should pass!
        ("MyParameter1", VALID),        # fully qualified
        ("/MyParameter2", VALID),       # fully qualified
        ("/Level-1/L2/L3/L4/L5/L6/L7/L8/L9/L10/L11/L12/L13/L14/parameter-name", VALID), # 15 level path
        ("/Level-1/L2/L3/L4/L5/L6/L7/L8/L9/L10/L11/L12/L13/L14/15/parameter-name", INVALID), # 16 level path
        ("/Level-1/L2/L3/L4/L5/L6/L7/L8/L9/L10/L11/L12/L13/L14/L15/L16/parameter-name", INVALID), # too deep path
        ("MyParameter3/L1", INVALID),   # not fully qualified
        ("awsTestParameter", INVALID),  # cannot start with aws
        ("/aws/testparam1", INVALID),   # cannot start with aws
        ("SSM-testparameter", INVALID), # cannot start with ssm
        ("/å", INVALID),   # bad characters
    ]
)
def test_is_valid_ssm_name(name, validity):
    is_valid, message = is_valid_ssm_name(name)
    assert is_valid == validity, message
