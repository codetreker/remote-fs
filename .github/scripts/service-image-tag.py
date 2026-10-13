#!/usr/bin/env python3
import base64
from datetime import UTC, datetime
import json
import os
import re
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


REPOSITORY = "codetreker/remote-fs"
TAGS_URL = f"https://ghcr.io/v2/{REPOSITORY}/tags/list?n=100"


def require_ghcr_url(url):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != "ghcr.io":
        raise ValueError("GHCR requests must use HTTPS on ghcr.io")
    return url


class GHCRRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, url):
        return super().redirect_request(request, response, code, message, headers, require_ghcr_url(url))


urlopen = build_opener(GHCRRedirect()).open


def registry_json(url, authorization):
    request = Request(require_ghcr_url(url), headers={"Authorization": authorization})
    with urlopen(request, timeout=30) as response:
        return json.load(response), response.headers.get("Link", "")


def read_tags(token):
    tags = []
    url = TAGS_URL
    while url:
        try:
            body, link = registry_json(url, f"Bearer {token}")
        except HTTPError as error:
            if error.code != 404 or url != TAGS_URL:
                raise
            errors = json.load(error)["errors"]
            if errors and all(entry["code"] == "NAME_UNKNOWN" for entry in errors):
                return []
            raise
        page = body["tags"]
        if not isinstance(page, list) or not all(isinstance(tag, str) for tag in page):
            raise ValueError("GHCR tags must be an array of strings")
        tags.extend(page)
        next_link = re.search(r'<([^>]+)>;\s*rel="next"', link)
        if link and not next_link:
            raise ValueError("GHCR pagination Link must identify the next page")
        url = urljoin(url, next_link[1]) if next_link else ""
        if url:
            require_ghcr_url(url)
    return tags


def next_image(tags, date, sha):
    pattern = re.compile(rf"{date}-([1-9][0-9]*)-[a-f0-9]{{8}}")
    sequences = [int(match[1]) for tag in tags if (match := pattern.fullmatch(tag))]
    sequence = max(sequences, default=0) + 1
    return f"ghcr.io/{REPOSITORY}:{date}-{sequence}-{sha[:8]}"


def main():
    credentials = f'{os.environ["GITHUB_ACTOR"]}:{os.environ["GITHUB_TOKEN"]}'
    basic = base64.b64encode(credentials.encode()).decode()
    query = urlencode({"service": "ghcr.io", "scope": f"repository:{REPOSITORY}:pull,push"})
    body, _ = registry_json(f"https://ghcr.io/token?{query}", f"Basic {basic}")
    token = body["token"]
    if not isinstance(token, str) or not token:
        raise ValueError("GHCR token must be a nonempty string")
    image = next_image(read_tags(token), datetime.now(UTC).strftime("%y%m%d"), os.environ["GITHUB_SHA"])
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"image={image}\n")


if __name__ == "__main__":
    main()
