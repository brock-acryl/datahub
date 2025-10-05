from pathlib import Path
from unittest import mock

import pytest

from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossaryFileSource,
)


@pytest.fixture(name="rdf_content")
def fixture_rdf_content() -> str:
    return """@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <http://example.org/> .

ex:glossary a skos:ConceptScheme ;
    skos:prefLabel "Example Glossary"@en .

ex:termA a skos:Concept ;
    skos:prefLabel "Term A"@en ;
    skos:definition "Definition for term A"@en ;
    skos:inScheme ex:glossary .

ex:termB a skos:Concept ;
    skos:prefLabel "Term B"@en ;
    skos:definition "Definition for term B"@en ;
    skos:broader ex:termA ;
    skos:inScheme ex:glossary .
"""


def test_load_glossary_config_from_local_rdf(tmp_path: Path, rdf_content: str) -> None:
    rdf_file = tmp_path / "glossary.ttl"
    rdf_file.write_text(rdf_content)

    config = BusinessGlossaryFileSource.load_glossary_config(rdf_file)

    assert config.version == "1"
    assert config.source == "Example Glossary"
    assert config.url is None

    term_names = [term.name for term in config.terms or []]
    assert sorted(term_names) == ["Term A", "Term B"]

    term_map = {term.name: term for term in config.terms or []}
    assert term_map["Term A"].description == "Definition for term A"
    assert term_map["Term B"].inherits == ["Term A"]


def test_load_glossary_config_from_remote_rdf(rdf_content: str) -> None:
    remote_url = "https://example.org/glossary.ttl"

    with mock.patch("requests.get") as mock_get:
        mock_response = mock.Mock()
        mock_response.text = rdf_content
        mock_response.raise_for_status = mock.Mock()
        mock_get.return_value = mock_response

        config = BusinessGlossaryFileSource.load_glossary_config(remote_url)

    mock_get.assert_called_once_with(remote_url, timeout=30)
    assert config.url == remote_url
    assert config.source == "Example Glossary"
