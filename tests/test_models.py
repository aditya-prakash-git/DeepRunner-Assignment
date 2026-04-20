import pytest
from pydantic import ValidationError

from src.models import DocumentIn


def test_document_in_rejects_empty_title():
    with pytest.raises(ValidationError):
        DocumentIn(title="", body="x")


def test_document_in_defaults():
    d = DocumentIn(title="t", body="b")
    assert d.tags == []
    assert d.metadata == {}


def test_document_in_accepts_full_payload():
    d = DocumentIn(
        title="Q1 Report",
        body="contents…",
        tags=["finance", "2026"],
        metadata={"author": "ada"},
    )
    assert d.tags == ["finance", "2026"]
    assert d.metadata["author"] == "ada"
