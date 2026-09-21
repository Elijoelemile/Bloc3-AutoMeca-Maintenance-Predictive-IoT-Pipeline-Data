"""Test fonctionnel — transform/materialized_view_telemetrie.sql contre un vrai ClickHouse.

Rejoue les verifications manuelles faites pendant la construction :
calcul RMS/tendance corrects (avec le fix lagInFrame sur la premiere
fenetre), et raffinement par fusion d'etats sur un deuxieme insert
dans la meme fenetre — le coeur du mecanisme "recalcule automatiquement
a chaque insertion" documente dans le README.
"""
from datetime import datetime


def _insert_telemetrie(ch_client, rows):
    ch_client.insert(
        "automeca.telemetrie",
        rows,
        column_names=["machine_id", "datetime_mes", "volt", "rotate", "pressure", "vibration"],
    )


def test_rms_and_tendance_are_computed_correctly(ch_client):
    machine_id = 301
    _insert_telemetrie(ch_client, [
        (machine_id, datetime(2030, 2, 1, 10, 0, 10), 170, 400, 100, 30),
        (machine_id, datetime(2030, 2, 1, 10, 0, 40), 170, 400, 110, 40),
        (machine_id, datetime(2030, 2, 1, 10, 1, 10), 170, 400, 130, 50),
    ])

    result = ch_client.query(
        "SELECT fenetre, round(rms_vibration, 3), tendance_pression "
        "FROM automeca.v_telemetrie_tendance WHERE machine_id = %(m)s ORDER BY fenetre",
        parameters={"m": machine_id},
    )
    rows = result.result_rows

    assert len(rows) == 2
    # fenetre 10:00 : vibration [30, 40] -> RMS = sqrt((30^2+40^2)/2)
    assert rows[0][1] == round(((900 + 1600) / 2) ** 0.5, 3)
    assert rows[0][2] == 0  # 1ere fenetre : pas de precedente -> tendance = 0 (fix lagInFrame)
    # fenetre 10:01 : vibration [50] -> RMS = 50 ; tendance = 130 - moyenne(100,110) = 25
    assert rows[1][1] == 50
    assert rows[1][2] == 25


def test_materialized_view_refines_on_second_insert_same_window(ch_client):
    machine_id = 302
    _insert_telemetrie(ch_client, [(machine_id, datetime(2030, 2, 2, 9, 0, 5), 170, 400, 100, 20)])

    first = ch_client.query(
        "SELECT rms_vibration FROM automeca.v_telemetrie_tendance WHERE machine_id = %(m)s",
        parameters={"m": machine_id},
    ).result_rows
    assert first[0][0] == 20

    _insert_telemetrie(ch_client, [(machine_id, datetime(2030, 2, 2, 9, 0, 40), 170, 400, 100, 40)])

    second = ch_client.query(
        "SELECT round(rms_vibration, 3) FROM automeca.v_telemetrie_tendance WHERE machine_id = %(m)s",
        parameters={"m": machine_id},
    ).result_rows
    assert second[0][0] == round(((400 + 1600) / 2) ** 0.5, 3)
