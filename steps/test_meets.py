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


def test_normalise_places():
    meets = [
        meet(),
        meet(uid="a", country="Mexico"),
        meet(uid="b", country="Atlantis"),
        meet(uid="c", country=""),
        meet(uid="d", country="Australia", state="NSW"),
        meet(uid="e", country="Turkiye", state="Istanbul"),
        meet(uid="f", state="Lousiana"),
        meet(uid="g", state="District Of Columbia"),
        meet(uid="h", country="Mexico", state="Estado de México"),
    ]
    b.normalise_places(meets)
    assert [(m.country, m.state, m.continent) for m in meets] == [
        ("United States", "", "North America"),
        ("Mexico", "", "North America"),
        ("Atlantis", "", "Other"),
        ("", "", "Other"),
        ("Australia", "New South Wales", "Oceania"),
        ("Türkiye", "İstanbul", "Asia"),
        ("United States", "Louisiana", "North America"),
        ("United States", "District of Columbia", "North America"),
        ("Mexico", "Estado de México", "North America"),
    ]
    single = [meet(country="Australia")]
    b.normalise_places(single)
    assert single[0].continent == "Oceania"
    assert b.to_frame(meets)["continent"].to_list()[:2] == ["North America", "North America"]


def test_dedupe():
    def at(uid, name, federation="X"):
        return meet(uid=uid, name=name, federation=federation, location="Olympus Athletic Club, Jacksonville", start=date(2026, 10, 31))

    pairs_same = [
        ("Gates of Hades", "USPC Gates of Hades"),
        ("Nine Lives Classic Waitlist", "Nine Lives Classic"),
        ("5th Annual Jim Phillips Memorial Classic", "2027 Jim Phillips Memorial Classic"),
        ("2026 USA Powerlifting Long Island Winter Showdwon III", "2026 USA Powerlifting Long Island Winter Showdown III"),
        ("Powerlifting United: THE Meet", "THE Meet"),
    ]
    pairs_distinct = [
        ("AaSK Open - Classic", "AaSK Open - Equipped"),
        ("Open Force Comtoise #2", "Open Force Comtoise #2 DC/BP 2026"),
        ("2027 USA Powerlifting Raw Nationals", "2027 USA Powerlifting Youth Nationals"),
        ("Showdown Amateur", "USPC Showdown Pro/AM"),
    ]
    for one, two in pairs_same:
        assert len(b.dedupe([at("1", one), at("2", two)])) == 1, (one, two)
    for one, two in pairs_distinct:
        assert len(b.dedupe([at("1", one), at("2", two)])) == 2, (one, two)
    # The copy with a federation wins
    assert [m.uid for m in b.dedupe([at("1", "Nine Lives Classic Waitlist", ""), at("2", "Nine Lives Classic")])] == ["2"]


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


def test_lmp_meet():
    event = {
        "id": "abc",
        "titulo": "Copa 30 Caballeros ",
        "fecha_limite": "2099-10-30",
        "lugar": {"ciudad": "Cordoba", "estado": "Veracruz"},
        "fecha_evento": {"dia": "14", "mes": "Noviembre", "año": "2099"},
    }
    m = b.lmp_meet(event)
    assert (m.uid, m.name, m.start, m.location, m.state, m.country, m.added) == ("lmp-abc", "Copa 30 Caballeros", date(2099, 11, 14), "Cordoba", "Veracruz", "Mexico", None)
    assert m.status == "OPEN · closes 30 Oct"
    assert b.lmp_meet(event | {"fecha_limite": "2000-01-01"}).status == "CLOSED"


def test_merge():
    base = [meet(name="2026 USA Powerlifting Freak Fest", start=date(2026, 10, 31))]
    dupe = meet(uid="lmp-1", name="USA Powerlifting Freak Fest 2026", start=date(2026, 11, 1))
    new = meet(uid="lmp-2", name="Copa 30 Caballeros", start=date(2026, 10, 31))
    assert [m.uid for m in b.merge(base, [dupe, new])] == ["plcom-1", "lmp-2"]


def test_merge_needs_same_country():
    base = [meet(name="2026 USA Powerlifting Beer City Classic", start=date(2026, 11, 15), country="USA")]
    saltillo = meet(uid="lmp-3", name="100% Strength powerlifting meet", start=date(2026, 11, 15), country="Mexico")
    assert [m.uid for m in b.merge(base, [saltillo])] == ["plcom-1", "lmp-3"]
