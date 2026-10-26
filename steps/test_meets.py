from datetime import date

import meets as b
from icalendar import Calendar


def meet(**overrides) -> b.Meet:
    fields = {
        "uid": "plcom-1",
        "name": "Test Open",
        "start": date(2027, 1, 30),
        "end": date(2027, 1, 31),
        "federation": "USA Powerlifting",
        "location": "Boston, MA, USA",
        "country": "USA",
        "link": "https://form.jotform.com/1",
        "listing": "https://powerlifting.com/event/test-open/",
        "added": date(2026, 10, 1),
    }
    return b.Meet(**(fields | overrides))


def test_apa_status():
    open_page = "<strong>Maximum number of entrants:</strong> 28 <a>Register</a>"
    assert b.apa_status(open_page) == "OPEN · cap 28"
    assert b.apa_status('<meta property="og:title" content="City Open - SOLD OUT!" /><a>Register</a>') == "SOLD OUT"
    assert b.apa_status('<meta property="og:title" content="Unity Games II - CANCELLED BY MEET DIRECTOR" />') == "CANCELLED"
    assert b.apa_status("Registrations for this event are closed") == "CLOSED"


def test_jotform_status():
    assert b.jotform_status('<form class="jotform-form"><p>LIMITED TO 70 LIFTERS</p>') == "OPEN · cap 70"
    assert b.jotform_status("<title>Sorry, this competition is now full</title>") == "SOLD OUT"
    assert b.jotform_status("<title>Registration is closed for this event</title>") == "CLOSED"
    assert b.jotform_status("<title>Error</title>") is None


def test_liftingcast_status():
    assert b.liftingcast_status([{"_id": "m1"}, {"_id": "l1"}, {"_id": "l2"}, {"_id": "d1"}]) == "2 entered"


def test_feeds():
    assert b.FEEDS["us"](meet())
    assert not b.FEEDS["europe"](meet())
    assert b.FEEDS["open"](meet(status="OPEN · cap 70"))
    assert not b.FEEDS["open"](meet(status="12 entered"))
    assert not b.FEEDS["open"](meet(status="SOLD OUT"))
    assert not b.FEEDS["open"](meet())


def test_feed_ics():
    ics = b.feed_ics("us", [meet(status="OPEN · cap 70", name="Señor, Open; Classic")])
    event = Calendar.from_ical(ics).walk("VEVENT")[0]
    assert str(event["summary"]) == "[OPEN · cap 70] Señor, Open; Classic"
    assert event.decoded("dtend") == date(2027, 2, 1)


def test_to_frame():
    df = b.to_frame([meet(), meet(uid="femepo-1", country="Mexico", federation="FEMEPO")])
    assert df["region"].to_list() == ["us", "mx"]


def test_mx_state_in():
    assert b.mx_state_in("Selectivo Estatal SLP") == "San Luis Potosí"
    assert b.mx_state_in("2do Torneo Estatal Selectivo de Querétaro") == "Querétaro"
    assert b.mx_state_in("Estatal Selectivo y Novatos jalisco 2026") == "Jalisco"
    assert b.mx_state_in("9no Campeonato Baja California Sur") == "Baja California Sur"
    assert b.mx_state_in("La Revolución del Poder") == ""


def test_classify():
    uspa = ["United States Powerlifting Association"]
    assert b.classify("USPA Tested and Open 2026 Connecticut State Championship", uspa, ["Classic Raw", "Full Power", "non tested", "Raw", "Single Ply", "Tested"]) == (
        "Both",
        ["Raw", "Single-ply", "Wraps"],
        ["Full power"],
    )
    assert b.classify("2026 USA Powerlifting Capital Fright Classic", ["USA Powerlifting"], ["Bench Only", "Deadlift Only", "Raw with Wraps", "Push/Pull", "Equipped"]) == (
        "Tested",
        ["Equipped", "Wraps"],
        ["Bench only", "Deadlift only", "Push/pull"],
    )
    assert b.classify("Gates of Hades", ["United States Powerlifting Coalition"], ["Multi Ply", "Raw Knee Sleeves"]) == ("Unknown", ["Multi-ply", "Raw"], [])
    assert b.classify("Bench for Boobs and Biceps", ["Revolution Powerlifting Syndicate"], ["Bench Press"]) == ("Unknown", [], ["Bench only"])
    assert b.classify("TORNEO DE PESO MUERTO", ["FEMEPO"], []) == ("Tested", [], ["Deadlift only"])
