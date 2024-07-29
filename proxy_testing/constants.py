from botocore.credentials import Credentials
from botocore.auth import SigV4Auth

TEST_CONTENT_TYPE = "text/plain;charset=utf-8"
DEFAULT_REGION = "us-east-1"

AWS_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
HEX_SIGNATURE_SIZE = len(
    SigV4Auth(Credentials("foo", "foo"), "s3", DEFAULT_REGION)._sign(b"a", "b", True)
)
LONG_TEXT = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et "
    "dolore magna aliqua. Viverra aliquet eget sit amet tellus cras adipiscing. Viverra mauris in aliquam sem "
    "fringilla. Facilisis mauris sit amet massa vitae. Mauris vitae ultricies leo integer malesuada. Sed "
    "libero enim sed faucibus turpis in eu mi bibendum. Lorem sed risus ultricies tristique nulla aliquet enim."
    " Quis blandit turpis cursus in hac habitasse platea dictumst quisque. Diam maecenas ultricies mi eget "
    "mauris pharetra et ultrices neque. Aliquam sem fringilla ut morbi."
)
