"""Pseudonymisation des identites operateur — RGPD (page 2 du diagramme).

"Dès l'ingestion, avant stockage" : avant qu'une identite reelle (nom,
matricule) soit ecrite dans datamart.dim_operateur, elle est remplacee
par un pseudonyme — hachage a cle (HMAC-SHA256), deterministe (le meme
operateur donne toujours le meme pseudonyme, necessaire pour les
jointures) mais non reversible sans la cle secrete (voir common.config.
PrivacyConfig, jamais en dur). Satisfait la definition RGPD de la
pseudonymisation (Art. 4(5)) : la donnee ne permet pas de remonter a la
personne sans "information supplementaire" (ici, la cle).

Pret mais pas encore cable dans un flux actif : dim_operateur (Bloc 2)
n'est pas peuplee, aucune source reelle de donnees operateur n'existe
dans le dataset Kaggle — meme statut que quality.detect_capteurs_
silencieux.

Le "Journal d'acces & traitements" (2e volet RGPD du diagramme, relie
en pointille — pas une etape du flux de donnees) n'est pas traite ici :
c'est une configuration d'infrastructure (extension pgaudit sur
PostgreSQL), pas du code applicatif — a activer au moment du
provisionnement de PostgreSQL manage (voir README).
"""
import hashlib
import hmac


def pseudonymiser(identifiant_reel: str, cle_secrete: bytes) -> str:
    """Pseudonyme deterministe (HMAC-SHA256, 64 caracteres hex), non
    reversible sans la cle secrete."""
    if not identifiant_reel:
        raise ValueError("identifiant_reel ne peut pas etre vide")
    digest = hmac.new(cle_secrete, identifiant_reel.encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()


def pseudonymiser_operateur(matricule: str, nom: str, cle_secrete: bytes) -> dict:
    """Pseudonymise les champs identifiants d'un enregistrement operateur
    avant ecriture dans datamart.dim_operateur — 'equipe' n'est pas une
    donnee identifiante en soi, elle est conservee telle quelle."""
    return {
        "matricule_pseudonymise": pseudonymiser(matricule, cle_secrete),
        "nom_pseudonymise": pseudonymiser(nom, cle_secrete),
    }
