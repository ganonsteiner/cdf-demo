"""
Files route — mirrors CDF Files API.

Files represent ET (Engineering Technology) documents: POH sections, ADs, SBs.
The /documents/{filename} endpoint is a custom extension to serve actual file
content from the data/documents/ directory — mirroring CDF's file download flow.
"""

from pathlib import Path
from typing import Any, Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from ..store.store import store, CdfFile

# Resolve data/documents relative to project root (3 levels up from this file)
DOCUMENTS_DIR = Path(__file__).parent.parent.parent / "data" / "documents"

router = APIRouter()


class FileFilter(BaseModel):
    assetIds: Optional[list[int]] = None
    assetExternalIds: Optional[list[str]] = None
    mimeType: Optional[str] = None
    metadata: Optional[dict[str, str]] = None


class FileListRequest(BaseModel):
    filter: Optional[FileFilter] = None
    limit: int = 1000
    cursor: Optional[str] = None


class FileByIdsRequest(BaseModel):
    items: list[dict[str, Any]]


class DownloadLinkRequest(BaseModel):
    items: list[dict[str, Any]]


def _resolve_asset_ids(asset_external_ids: list[str]) -> list[int]:
    result = []
    for ext_id in asset_external_ids:
        asset = store.get_asset(ext_id)
        if asset:
            result.append(asset.id)
    return result


def _apply_filter(files: list[CdfFile], f: Optional[FileFilter]) -> list[CdfFile]:
    if not f:
        return files
    result = files
    asset_ids: set[int] = set()
    if f.assetIds:
        asset_ids.update(f.assetIds)
    if f.assetExternalIds:
        asset_ids.update(_resolve_asset_ids(f.assetExternalIds))
    if asset_ids:
        result = [fi for fi in result if any(aid in asset_ids for aid in fi.assetIds)]
    if f.mimeType:
        result = [fi for fi in result if fi.mimeType == f.mimeType]
    if f.metadata:
        result = [
            fi for fi in result
            if all(fi.metadata.get(k) == v for k, v in f.metadata.items())
        ]
    return result


@router.post("/files/list")
def list_files(body: FileListRequest) -> dict[str, Any]:
    """POST /files/list — mirrors CDF Files.list()."""
    all_files = store.get_files()
    filtered = _apply_filter(all_files, body.filter)
    offset = 0
    if body.cursor:
        try:
            offset = int(body.cursor)
        except ValueError:
            offset = 0
    page = filtered[offset: offset + body.limit]
    next_cursor = str(offset + body.limit) if offset + body.limit < len(filtered) else None
    return {"items": [f.model_dump() for f in page], "nextCursor": next_cursor}


@router.post("/files/byids")
def get_files_by_ids(body: FileByIdsRequest) -> dict[str, Any]:
    """POST /files/byids — mirrors CDF Files.retrieve()."""
    result = []
    for item in body.items:
        ext_id = item.get("externalId")
        if ext_id:
            f = store.get_file(str(ext_id))
            if f:
                result.append(f.model_dump())
    return {"items": result}


@router.post("/files/downloadlink")
def get_download_links(body: DownloadLinkRequest) -> dict[str, Any]:
    """
    POST /files/downloadlink — mirrors CDF Files.getDownloadUrls().

    Returns a local URL pointing to the /documents/{filename} endpoint
    instead of a presigned S3 URL.
    """
    items = []
    for item in body.items:
        ext_id = item.get("externalId")
        if ext_id:
            f = store.get_file(str(ext_id))
            if f:
                filename = f.metadata.get("filename", f.name)
                items.append({
                    "externalId": ext_id,
                    "downloadUrl": f"http://localhost:4001/documents/{filename}",
                })
    return {"items": items}


@router.get("/documents/{filename}", response_class=PlainTextResponse)
def serve_document(filename: str) -> str:
    """
    GET /documents/{filename} — custom endpoint to serve text document content.

    This is not part of the real CDF API but provides the document download
    capability that CDF's presigned URLs would normally serve via S3.
    """
    doc_path = DOCUMENTS_DIR / filename
    if not doc_path.exists():
        raise HTTPException(status_code=404, detail=f"Document not found: {filename}")
    return doc_path.read_text()
