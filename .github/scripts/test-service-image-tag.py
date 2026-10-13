#!/usr/bin/env python3
import base64
from contextlib import redirect_stdout
from datetime import UTC, datetime
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request

sys.dont_write_bytecode = True
script = Path(__file__).with_name("service-image-tag.py")
spec = importlib.util.spec_from_file_location("service_image_tag", script)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def response(body, link=""):
    result = io.BytesIO(json.dumps(body).encode())
    result.headers = {"Link": link}
    return result


def http_error(code, registry_code="DENIED", url=release.TAGS_URL):
    body = json.dumps({"errors": [{"code": registry_code}]}).encode()
    return HTTPError(url, code, "registry failure", {}, io.BytesIO(body))


class ImageTagTests(unittest.TestCase):
    def test_day_without_published_tags_starts_at_one(self):
        tags = ["261012-999-deadbeef", "latest", "261013-3", "261013-0-deadbeef",
                "261013-01-deadbeef", "261013-99-DEADBEEF", "261013-4-deadbeef-extra"]
        self.assertEqual(release.next_image(tags, "261013", "0123abcdef012345"),
                         "ghcr.io/codetreker/remote-fs:261013-1-0123abcd")

    def test_daily_maximum_spans_commits_and_retry_after_publication(self):
        tags = ["261013-2-aaaaaaaa", "261013-9-bbbbbbbb", "261012-999-cccccccc"]
        image = release.next_image(tags, "261013", "0123abcdef012345")
        self.assertEqual(image, "ghcr.io/codetreker/remote-fs:261013-10-0123abcd")
        self.assertEqual(release.next_image(tags + [image.split(":")[-1]], "261013", "0123abcdef012345"),
                         "ghcr.io/codetreker/remote-fs:261013-11-0123abcd")

    def test_pagination_supplies_all_tags_to_counter(self):
        next_url = "https://ghcr.io/v2/codetreker/remote-fs/tags/list?n=100&last=261013-2-aaaaaaaa"
        pages = [response({"tags": ["261013-2-aaaaaaaa"]},
                          '</v2/codetreker/remote-fs/tags/list?n=100&last=261013-2-aaaaaaaa>; rel="next"'),
                 response({"tags": ["261013-15-bbbbbbbb"]})]
        with patch.object(release, "urlopen", side_effect=pages) as request:
            tags = release.read_tags("registry-fixture-token")
        self.assertEqual(release.next_image(tags, "261013", "0123abcdef"),
                         "ghcr.io/codetreker/remote-fs:261013-16-0123abcd")
        self.assertEqual([call.args[0].full_url for call in request.call_args_list],
                         [release.TAGS_URL, next_url])
        self.assertTrue(all(call.args[0].get_header("Authorization") == "Bearer registry-fixture-token"
                            for call in request.call_args_list))

    def test_initial_missing_repository_is_empty(self):
        with patch.object(release, "urlopen", side_effect=http_error(404, "NAME_UNKNOWN")):
            self.assertEqual(release.read_tags("registry-fixture-token"), [])

    def test_registry_and_network_errors_propagate(self):
        errors = [http_error(401), http_error(403), http_error(404), URLError("network failure")]
        for error in errors:
            with self.subTest(error=error), patch.object(release, "urlopen", side_effect=error):
                with self.assertRaises(type(error)) as raised:
                    release.read_tags("registry-fixture-token")
                self.assertIs(raised.exception, error)
        with patch.object(release, "urlopen", side_effect=[
                response({"tags": ["261013-2-aaaaaaaa"]}, '</v2/next>; rel="next"'),
                http_error(404, "NAME_UNKNOWN", "https://ghcr.io/v2/next")]):
            with self.assertRaises(HTTPError):
                release.read_tags("registry-fixture-token")

    def test_unknown_or_malformed_json_is_rejected(self):
        invalid_json = io.BytesIO(b"not-json")
        invalid_json.headers = {}
        cases = [(invalid_json, json.JSONDecodeError),
                 (response({"tags": None}), ValueError),
                 (response({"tags": [3]}), ValueError),
                 (response({}), KeyError)]
        for page, error in cases:
            with self.subTest(error=error), patch.object(release, "urlopen", return_value=page):
                with self.assertRaises(error):
                    release.read_tags("registry-fixture-token")

    def test_pagination_never_forwards_credentials_outside_https_registry(self):
        for url in ["https://other.invalid/v2/tags", "http://ghcr.io/v2/tags",
                    "https://ghcr.io@other.invalid/v2/tags"]:
            with self.subTest(url=url), patch.object(release, "urlopen", return_value=response(
                    {"tags": []}, f'<{url}>; rel="next"')) as request:
                with self.assertRaises(ValueError):
                    release.read_tags("registry-fixture-token")
                self.assertEqual(request.call_count, 1)

    def test_invalid_pagination_link_fails_before_counter_allocation(self):
        with patch.object(release, "urlopen", return_value=response({"tags": []}, "malformed-link")):
            with self.assertRaises(ValueError):
                release.read_tags("registry-fixture-token")

    def test_redirects_and_direct_requests_keep_credentials_on_registry(self):
        request = Request(release.TAGS_URL, headers={"Authorization": "Bearer registry-fixture-token"})
        redirect = release.GHCRRedirect()
        allowed = redirect.redirect_request(request, None, 302, "Found", {}, "https://ghcr.io/v2/next")
        self.assertEqual(allowed.get_header("Authorization"), "Bearer registry-fixture-token")
        for url in ["https://other.invalid/v2/next", "http://ghcr.io/v2/next"]:
            with self.subTest(url=url), patch.object(release, "urlopen") as open_request:
                with self.assertRaises(ValueError):
                    redirect.redirect_request(request, None, 302, "Found", {}, url)
                with self.assertRaises(ValueError):
                    release.registry_json(url, "Bearer registry-fixture-token")
                open_request.assert_not_called()

    def test_invalid_token_exchange_never_writes_image_output(self):
        with patch.dict(os.environ, {"GITHUB_ACTOR": "test-actor", "GITHUB_TOKEN": "github-fixture-token"}, clear=True):
            for token in ["", None, 3]:
                with self.subTest(token=token), patch.object(release, "urlopen", return_value=response({"token": token})):
                    with self.assertRaises(ValueError):
                        release.main()

    def test_main_exchanges_token_and_writes_only_image_output(self):
        class Clock:
            @staticmethod
            def now(timezone):
                self.assertIs(timezone, UTC)
                return datetime(2026, 10, 13, 23, 59, tzinfo=UTC)

        repository_root = script.parents[2]
        (repository_root / ".tmp").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="image-tag-test.", dir=repository_root / ".tmp") as temporary:
            output = Path(temporary) / "output"
            output.write_text("existing=value\n")
            environment = {"GITHUB_ACTOR": "test-actor", "GITHUB_TOKEN": "github-fixture-token",
                           "GITHUB_SHA": "0123abcdef012345", "GITHUB_OUTPUT": str(output)}
            pages = [response({"token": "registry-fixture-token"}),
                     response({"tags": ["261013-5-aaaaaaaa"]})]
            stdout = io.StringIO()
            with patch.dict(os.environ, environment, clear=True), patch.object(release, "datetime", Clock), \
                    patch.object(release, "urlopen", side_effect=pages) as request, redirect_stdout(stdout):
                release.main()
            self.assertEqual(output.read_text(),
                             "existing=value\nimage=ghcr.io/codetreker/remote-fs:261013-6-0123abcd\n")
            self.assertEqual(stdout.getvalue(), "")
            token_request, tags_request = [call.args[0] for call in request.call_args_list]
            basic = base64.b64encode(b"test-actor:github-fixture-token").decode()
            self.assertEqual(token_request.get_header("Authorization"), f"Basic {basic}")
            self.assertEqual(tags_request.get_header("Authorization"), "Bearer registry-fixture-token")
            self.assertEqual(token_request.full_url,
                             "https://ghcr.io/token?service=ghcr.io&scope=repository%3Acodetreker%2Fremote-fs%3Apull%2Cpush")


if __name__ == "__main__":
    unittest.main(verbosity=2)
