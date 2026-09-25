"""Tests unitaires — privacy/pseudonymisation.py

Fonctions pures, aucune simulation necessaire.
"""
import pytest

from privacy.pseudonymisation import pseudonymiser, pseudonymiser_operateur

CLE_A = b"cle-secrete-test-a"
CLE_B = b"cle-secrete-test-b"


def test_pseudonymiser_is_deterministic():
    assert pseudonymiser("OP-042", CLE_A) == pseudonymiser("OP-042", CLE_A)


def test_pseudonymiser_different_key_gives_different_pseudonym():
    assert pseudonymiser("OP-042", CLE_A) != pseudonymiser("OP-042", CLE_B)


def test_pseudonymiser_different_input_gives_different_pseudonym():
    assert pseudonymiser("OP-042", CLE_A) != pseudonymiser("OP-043", CLE_A)


def test_pseudonymiser_output_is_64_hex_chars():
    pseudonyme = pseudonymiser("OP-042", CLE_A)
    assert len(pseudonyme) == 64
    int(pseudonyme, 16)  # leve ValueError si ce n'est pas de l'hexadecimal


def test_pseudonymiser_raises_on_empty_input():
    with pytest.raises(ValueError):
        pseudonymiser("", CLE_A)


def test_pseudonymiser_operateur_returns_both_pseudonymized_fields():
    result = pseudonymiser_operateur("OP-042", "Jean Dupont", CLE_A)

    assert result == {
        "matricule_pseudonymise": pseudonymiser("OP-042", CLE_A),
        "nom_pseudonymise": pseudonymiser("Jean Dupont", CLE_A),
    }
    assert result["matricule_pseudonymise"] != result["nom_pseudonymise"]
