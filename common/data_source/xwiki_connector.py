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
import os
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
        # Azure managed-identity OAuth: fetch a bearer token for the configured
        # resource/audience so the connector can pass Container Apps EasyAuth
        # (Entra) without a static credential. Enabled via the connector config
        # or env (XWIKI_OAUTH_RESOURCE), so the RAGFlow UI only needs wiki_base.
        mi_resource = credentials.get("azure_oauth_resource") or os.environ.get("XWIKI_OAUTH_RESOURCE")
        mi_client_id = credentials.get("azure_mi_client_id") or os.environ.get("XWIKI_OAUTH_MI_CLIENT_ID")
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        elif username and password is not None:
            self._auth = requests.auth.HTTPBasicAuth(str(username), str(password))
        elif mi_resource:
            self._headers["Authorization"] = f"Bearer {self._fetch_msi_token(mi_resource, mi_client_id)}"
        return None

    def _fetch_msi_token(self, resource: str, client_id: Optional[str] = None) -> str:
        """Fetch an Entra access token from the platform managed-identity endpoint.

        Supports Azure Container Apps / App Service (``IDENTITY_ENDPOINT`` +
        ``IDENTITY_HEADER``) and falls back to IMDS. ``resource`` is the target
        audience (e.g. ``api://<app-id>``); ``client_id`` selects a specific
        user-assigned identity (omit for the system-assigned one). Fetched per
        sync, so tokens stay fresh without manual rotation.
        """
        identity_endpoint = os.environ.get("IDENTITY_ENDPOINT")
        identity_header = os.environ.get("IDENTITY_HEADER")
        if identity_endpoint and identity_header:
            params: Dict[str, str] = {"resource": resource, "api-version": "2019-08-01"}
            if client_id:
                params["client_id"] = client_id
            resp = requests.get(
                identity_endpoint,
                headers={"X-IDENTITY-HEADER": identity_header},
                params=params,
                timeout=self.request_timeout,
            )
        else:  # IMDS fallback (VMs / classic MSI)
            params = {"resource": resource, "api-version": "2018-02-01"}
            if client_id:
                params["client_id"] = client_id
            resp = requests.get(
                "http://169.254.169.254/metadata/identity/oauth2/token",
                headers={"Metadata": "true"},
                params=params,
                timeout=self.request_timeout,
            )
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            raise ConnectorMissingCredentialError(
                f"Managed-identity token fetch failed ({resp.status_code}): {resp.text[:200]}"
            ) from exc
        tok = resp.json().get("access_token")
        if not tok:
            raise ConnectorMissingCredentialError("Managed-identity response contained no access_token.")
        return tok

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

    def _iter_pages_at(self, base: str) -> Generator[str, None, None]:
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

    def _space_pages_urls(self) -> List[str]:
        """In space mode, return the pages-collection URL for the root space plus
        every nested descendant space.

        XWiki lists nested spaces flat under ``/wikis/<wiki>/spaces``; each entry
        carries a ``rel=.../pages`` link whose href encodes its full hierarchical
        path (``.../spaces/Root/spaces/Child/pages``). The hierarchical
        ``/spaces/<root>/spaces`` sub-resource does *not* enumerate descendants,
        so we filter the flat listing by path prefix instead.
        """
        segments = [quote(s, safe="") for s in self.space.split(".") if s]
        root_path = "/spaces/" + "/spaces/".join(segments)       # .../spaces/A/spaces/B
        descendant_marker = root_path + "/spaces/"               # children live below this
        root_pages = self._rest(f"wikis/{self.wiki}{root_path}/pages")
        urls: List[str] = [root_pages]
        seen = {root_pages}
        spaces_url = self._rest(f"wikis/{self.wiki}/spaces")
        start = 0
        while True:
            payload = self._get_json(spaces_url, params={"start": start, "number": self.page_size})
            spaces = payload.get("spaces", []) if isinstance(payload, dict) else []
            spaces = [s for s in spaces if isinstance(s, dict)]
            if not spaces:
                break
            for space in spaces:
                href = None
                for link in space.get("links", []) or []:
                    if str(link.get("rel", "")).endswith("/rel/pages"):
                        href = link.get("href")
                        break
                if href and descendant_marker in href and href not in seen:
                    seen.add(href)
                    urls.append(href)
            if len(spaces) < self.page_size:
                break
            start += self.page_size
        return urls

    def _iter_page_links(self) -> Generator[str, None, None]:
        if self.index_mode == "space" and self.space:
            for pages_url in self._space_pages_urls():
                yield from self._iter_pages_at(pages_url)
        else:
            yield from self._iter_pages_at(self._rest(f"wikis/{self.wiki}/pages"))

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
        url = page.get("xwikiAbsoluteUrl") or page.get("xwikiRelativeUrl")

        # Prepend the source link into the body so it rides inside the indexed
        # content and renders as a clickable link in RAGFlow's chunk/citation
        # viewer (RAGFlow does not surface document meta_fields in the retrieval
        # UI, so the URL has to travel in the content itself). Kept in metadata
        # too, for retrieval-time filtering / API consumers.
        header = f"# {title}\n\n" if title else ""
        source_line = f"[View in XWiki »]({url})\n\n" if url else ""
        body = f"{header}{source_line}{content}".encode("utf-8")
        updated_at = self._parse_modified(page.get("modified"))

        metadata: Dict[str, Any] = {}
        for key in ("space", "wiki", "version", "author"):
            if page.get(key):
                metadata[key] = str(page[key])
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
