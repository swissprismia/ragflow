"""XWiki data source connector.

Indexes pages from an XWiki instance through its REST API. Unlike the generic
REST API connector, this connector has no SSRF guard, so it can reach
internal/private XWiki servers (the common deployment for a corporate wiki).

Configuration (stored on the connector row, under ``config``)::

    {
        "wiki_base": "https://wiki.example.com/xwiki",  # no trailing /rest, no trailing slash
        "wiki": "xwiki",                                # wiki id (default "xwiki")
        "space": "",                                    # optional: restrict to one top-level space key
        "index_mode": "everything",                     # "everything" | "space"
        "batch_size": 16,
        "credentials": {                                # optional
            "username": "...", "password": "...",       # HTTP Basic, or
            "token": "..."                              # Bearer token
        }
    }
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import quote

import requests

from common.data_source.config import INDEX_BATCH_SIZE, DocumentSource
from common.data_source.exceptions import (
    ConnectorMissingCredentialError,
    ConnectorValidationError,
)
from common.data_source.interfaces import (
    LoadConnector,
    PollConnector,
    SecondsSinceUnixEpoch,
)
from common.data_source.models import Document
from common.data_source.utils import retry_builder

logger = logging.getLogger(__name__)


class XWikiConnector(LoadConnector, PollConnector):
    """Pulls XWiki pages via REST and yields RAGFlow ``Document`` batches."""

    def __init__(
        self,
        wiki_base: str,
        wiki: str = "xwiki",
        space: str = "",
        index_mode: str = "everything",
        batch_size: int = INDEX_BATCH_SIZE,
        request_timeout: int = 60,
        page_size: int = 100,
        skip_hidden: bool = True,
    ) -> None:
        if not wiki_base:
            raise ConnectorValidationError("XWiki connector requires 'wiki_base'.")
        self.wiki_base = str(wiki_base).rstrip("/")
        self.wiki = (wiki or "xwiki").strip()
        self.space = (space or "").strip()
        self.index_mode = (index_mode or "everything").lower()
        self.batch_size = int(batch_size) if batch_size else INDEX_BATCH_SIZE
        if self.batch_size <= 0:
            self.batch_size = INDEX_BATCH_SIZE
        self.request_timeout = request_timeout
        self.page_size = page_size if page_size and page_size > 0 else 100
        self.skip_hidden = skip_hidden

        self._auth: Optional[requests.auth.HTTPBasicAuth] = None
        self._headers: Dict[str, str] = {"Accept": "application/json"}

    # -- Credentials --------------------------------------------------------

    def load_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any] | None:
        credentials = credentials or {}
        token = credentials.get("token") or credentials.get("api_key")
        username = credentials.get("username")
        password = credentials.get("password")
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        elif username and password is not None:
            self._auth = requests.auth.HTTPBasicAuth(str(username), str(password))
        return None

    # -- HTTP helpers -------------------------------------------------------

    def _rest(self, path: str) -> str:
        return f"{self.wiki_base}/rest/{path.lstrip('/')}"

    @retry_builder(
        tries=4, delay=1, max_delay=20, backoff=2,
        exceptions=(requests.ConnectionError, requests.Timeout, requests.HTTPError),
    )
    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = requests.get(
            url, headers=self._headers, auth=self._auth,
            params=params, timeout=self.request_timeout,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (401, 403):
                raise ConnectorMissingCredentialError(
                    f"XWiki authentication failed with status {status}"
                ) from exc
            if status is not None and 400 <= status < 500 and status != 429:
                raise ConnectorValidationError(
                    f"XWiki request to {url} failed with client error {status}"
                ) from exc
            raise
        try:
            return resp.json()
        except ValueError as exc:
            raise ConnectorValidationError(
                f"XWiki response from {url} is not valid JSON (is the URL correct?)"
            ) from exc

    # -- Page enumeration ---------------------------------------------------

    def _pages_collection_url(self) -> str:
        if self.index_mode == "space" and self.space:
            # XWiki nested-space path: a dotted key "A.B" maps to spaces/A/spaces/B
            segments = [quote(s, safe="") for s in self.space.split(".") if s]
            space_path = "/spaces/".join(segments)
            return self._rest(f"wikis/{self.wiki}/spaces/{space_path}/pages")
        return self._rest(f"wikis/{self.wiki}/pages")

    @staticmethod
    def _summaries(payload: Any) -> List[Dict[str, Any]]:
        """Extract the page-summary list, tolerant of response shape."""
        if isinstance(payload, dict):
            summaries = payload.get("pageSummaries")
            if isinstance(summaries, list):
                return [s for s in summaries if isinstance(s, dict)]
            for value in payload.values():
                if isinstance(value, list) and all(isinstance(v, dict) for v in value):
                    return value
        if isinstance(payload, list):
            return [s for s in payload if isinstance(s, dict)]
        return []

    @staticmethod
    def _page_link(summary: Dict[str, Any]) -> Optional[str]:
        for link in summary.get("links", []) or []:
            if not isinstance(link, dict):
                continue
            rel = str(link.get("rel", ""))
            # rel is e.g. "http://www.xwiki.org/rel/page"
            if rel.endswith("/rel/page") or rel.endswith("/page"):
                return link.get("href")
        return None

    def _iter_page_links(self) -> Generator[str, None, None]:
        base = self._pages_collection_url()
        start = 0
        while True:
            payload = self._get_json(base, params={"start": start, "number": self.page_size})
            summaries = self._summaries(payload)
            if not summaries:
                break
            for summary in summaries:
                href = self._page_link(summary)
                if href:
                    yield href
            if len(summaries) < self.page_size:
                break
            start += self.page_size

    # -- Page → Document ----------------------------------------------------

    def _fetch_document(self, href: str) -> Optional[Document]:
        page = self._get_json(href)
        if not isinstance(page, dict):
            return None

        if self.skip_hidden and page.get("hidden"):
            return None

        content = (page.get("content") or "").strip()
        if not content:
            return None

        title = page.get("title") or page.get("name") or page.get("fullName") or ""
        full_name = page.get("fullName") or page.get("id") or href
        body = (f"# {title}\n\n{content}" if title else content).encode("utf-8")
        updated_at = self._parse_modified(page.get("modified"))

        metadata: Dict[str, Any] = {}
        for key in ("space", "wiki", "version", "author"):
            if page.get(key):
                metadata[key] = str(page[key])
        url = page.get("xwikiAbsoluteUrl") or page.get("xwikiRelativeUrl")
        if url:
            metadata["url"] = str(url)

        return Document(
            id=str(full_name),
            source=DocumentSource.XWIKI,
            semantic_identifier=str(title or full_name),
            extension=".md",
            blob=body,
            doc_updated_at=updated_at,
            size_bytes=len(body),
            metadata=metadata or None,
        )

    @staticmethod
    def _parse_modified(value: Any) -> datetime:
        now = datetime.now(timezone.utc)
        if value is None:
            return now
        if isinstance(value, (int, float)):
            seconds = float(value)
            if seconds > 1e12:  # epoch milliseconds
                seconds /= 1000.0
            try:
                return datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return now
        if isinstance(value, str):
            text = value.strip()
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(text, fmt)
                    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return now

    # -- Generation ---------------------------------------------------------

    def _yield_documents(
        self, time_window: Optional[tuple[datetime, datetime]]
    ) -> Generator[List[Document], None, None]:
        batch: List[Document] = []
        for href in self._iter_page_links():
            try:
                doc = self._fetch_document(href)
            except (ConnectorMissingCredentialError, ConnectorValidationError):
                raise
            except Exception as exc:
                logger.warning("XWiki: failed to fetch page %s: %s", href, exc)
                continue
            if doc is None:
                continue
            if time_window is not None:
                start, end = time_window
                if not (start <= doc.doc_updated_at < end):
                    continue
            batch.append(doc)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def load_from_state(self) -> Generator[List[Document], None, None]:
        return self._yield_documents(None)

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> Generator[List[Document], None, None]:
        return self._yield_documents(
            (
                datetime.fromtimestamp(start, tz=timezone.utc),
                datetime.fromtimestamp(end, tz=timezone.utc),
            )
        )

    # -- Validation ---------------------------------------------------------

    def validate_connector_settings(self) -> None:
        try:
            self._get_json(self._rest(f"wikis/{self.wiki}"))
        except (ConnectorMissingCredentialError, ConnectorValidationError):
            raise
        except Exception as exc:
            raise ConnectorValidationError(f"XWiki validation failed: {exc}") from exc
