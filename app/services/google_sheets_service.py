"""Fetch a public Google Sheets tab as CSV for the existing dataset upload pipeline."""

from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen
import re

from app.config.config import settings
from app.utils.responses import error_response


GOOGLE_SHEETS_HOST = "docs.google.com"
GOOGLE_SHEET_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{20,}$")
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class GoogleSheetExport:
    file_name: str
    source_path: Path
    file_size: int

    def cleanup(self) -> None:
        self.source_path.unlink(missing_ok=True)


def _invalid_url() -> None:
    raise error_response(
        status_code=400,
        detail="Enter a valid Google Sheets link from docs.google.com/spreadsheets/d/...",
    )


def _google_sheet_id_and_gid(sheet_url: str) -> tuple[str, str]:
    parsed = urlparse(sheet_url.strip())
    if parsed.scheme != "https" or parsed.hostname != GOOGLE_SHEETS_HOST:
        _invalid_url()

    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts or path_parts[0] != "spreadsheets":
        _invalid_url()
    try:
        sheet_id_index = path_parts.index("d")
        spreadsheet_id = path_parts[sheet_id_index + 1]
    except (ValueError, IndexError):
        _invalid_url()
    if not GOOGLE_SHEET_ID_PATTERN.fullmatch(spreadsheet_id):
        _invalid_url()

    query = parse_qs(parsed.query)
    fragment_query = parse_qs(parsed.fragment)
    gid = (query.get("gid") or fragment_query.get("gid") or ["0"])[0]
    if not gid.isdigit():
        _invalid_url()
    return spreadsheet_id, gid


def _download_url(spreadsheet_id: str, gid: str) -> str:
    return (
        f"https://{GOOGLE_SHEETS_HOST}/spreadsheets/d/{quote(spreadsheet_id, safe='')}/export?"
        f"{urlencode({'format': 'csv', 'gid': gid})}"
    )


def _download_error(exc: HTTPError) -> None:
    if exc.code in {401, 403}:
        raise error_response(
            status_code=403,
            detail="This Google Sheet is private or inaccessible. Set the selected sheet to 'Anyone with the link can view'.",
        ) from exc
    if exc.code == 404:
        raise error_response(status_code=400, detail="The Google Sheet link or selected tab was not found") from exc
    raise error_response(status_code=502, detail="Google Sheets could not be reached. Please try again.") from exc


def download_google_sheet_csv(*, sheet_url: str, max_file_size_bytes: int | None) -> GoogleSheetExport:
    """Download one public Google Sheets tab without accepting an arbitrary fetch URL."""
    spreadsheet_id, gid = _google_sheet_id_and_gid(sheet_url)
    request = Request(
        _download_url(spreadsheet_id, gid),
        headers={"User-Agent": "ArithLab Google Sheets importer"},
    )
    temp_file = NamedTemporaryFile(prefix="google_sheet_", suffix=".csv", delete=False)
    source_path = Path(temp_file.name)
    downloaded_size = 0
    try:
        try:
            with urlopen(request, timeout=settings.GOOGLE_SHEETS_FETCH_TIMEOUT_SECONDS) as response, temp_file:
                content_type = response.headers.get_content_type().lower()
                if content_type in {"text/html", "application/xhtml+xml"}:
                    raise error_response(
                        status_code=403,
                        detail="This Google Sheet is private or inaccessible. Set the selected sheet to 'Anyone with the link can view'.",
                    )
                while chunk := response.read(DOWNLOAD_CHUNK_SIZE):
                    downloaded_size += len(chunk)
                    if max_file_size_bytes is not None and downloaded_size > max_file_size_bytes:
                        raise error_response(
                            status_code=400,
                            detail="Google Sheet exceeds your current plan file size limit. Please upgrade your plan.",
                        )
                    temp_file.write(chunk)
        except HTTPError as exc:
            _download_error(exc)
        except (URLError, TimeoutError, OSError) as exc:
            raise error_response(
                status_code=502,
                detail="Google Sheets could not be reached. Check the link and try again.",
            ) from exc

        if not downloaded_size:
            raise error_response(status_code=400, detail="The selected Google Sheet is empty")
        return GoogleSheetExport(
            file_name=f"Google Sheet {spreadsheet_id[:8]}.csv",
            source_path=source_path,
            file_size=downloaded_size,
        )
    except Exception:
        source_path.unlink(missing_ok=True)
        raise
