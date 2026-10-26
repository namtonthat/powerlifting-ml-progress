"""Land upcoming powerlifting meets in S3 as parquet, plus subscribable .ics feeds."""

import difflib
import html
import logging
import re
import time
import unicodedata
from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlparse

import boto3
import common_io
import conf
import country_converter as coco
import polars as pl
import requests
from icalendar import Calendar, Event
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "powerlifting-ml-progress meets (+https://github.com/namtonthat/powerlifting-ml-progress)"
# powerlifting.com sometimes 504s on a slow page until its cache warms, so back off for up to about 90s
SESSION.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=3, status_forcelist=(502, 503, 504))))
# LiftingCast allows about 100 meet lookups, then 429s for a long while; pace it and stop at the first 429.
LIFTINGCAST_PAUSE = 3
liftingcast_blocked = False

NOT_OPEN = ("SOLD OUT", "CLOSED", "CANCELLED")
# powerlifting.com's sport categories are patchy, so drop the other sports and non-meets instead
NOT_A_MEET = re.compile(r"strongman|weightlifting|\bIWF\b|strongfirst|kettlebell|highland games|strict curl|clinic|certification|seminar|mock meet", re.IGNORECASE)
# ...unless the meet name says it also runs the powerlifting lifts, e.g. "Bench Press-Deadlift-Strict Curl"
LIFTS = re.compile(r"powerlift|bench|deadlift|squat", re.IGNORECASE)
# FEMEPO leaves state blank on some meets but names it in the title, e.g. "Selectivo Estatal SLP"
MX_STATES = {
    **{s: s for s in (
        "Aguascalientes", "Baja California Sur", "Baja California", "Campeche", "Chiapas", "Chihuahua", "Ciudad de México", "Coahuila",
        "Colima", "Durango", "Estado de México", "Guanajuato", "Guerrero", "Hidalgo", "Jalisco", "Michoacán", "Morelos", "Nayarit",
        "Nuevo León", "Oaxaca", "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí", "Sinaloa", "Sonora", "Tabasco", "Tamaulipas",
        "Tlaxcala", "Veracruz", "Yucatán", "Zacatecas",
    )},
    "SLP": "San Luis Potosí", "CDMX": "Ciudad de México", "EDOMEX": "Estado de México",
}  # fmt: skip

# Drug-tested by federation rule (IPF and its listed affiliates, plus drug-free federations); others need meet-level evidence
TESTED_FEDS = {
    "USA Powerlifting", "Powerlifting America", "International Powerlifting Federation", "Australian Powerlifting Alliance", "FEMEPO",
    "British Powerlifting", "Powerlifting Australia", "100% RAW Powerlifting Federation", "Natural Athlete Strength Association",
    "NASA Powerlifting", "Amateur American Powerlifting Federation",
}  # fmt: skip
UNTESTED_FEDS = {"World Powerlifting Congress", "Southern Powerlifting Federation", "American Powerlifting Federation"}
UNTESTED = re.compile(r"\bun-?tested\b|\bnon[- ]?tested\b|\btested (?:and|&) open\b|\bopen (?:and|&) tested\b", re.IGNORECASE)
TESTED = re.compile(r"\btested\b", re.IGNORECASE)
# First match per category wins, so "Raw with Wraps" is Wraps and not Raw
EQUIPMENT = (("Wraps", r"wrap"), ("Single-ply", r"single[- ]?ply"), ("Multi-ply", r"multi[- ]?ply"), ("Equipped", r"^equip(ped|ment)$"), ("Raw", r"\braw\b|\bclassic\b|unequipped|equipment[- ]free|sleeves"))  # fmt: skip
EVENTS = (("Push/pull", r"push.?pull"), ("Bench only", r"bench( press)? only|single lift bench"), ("Deadlift only", r"deadlift only|single lift deadlift"), ("Squat only", r"squat only"), ("Full power", r"full (power|meet|powerlifting)|3[- ]lift|classic powerlifting"))  # fmt: skip


@dataclass
class Meet:
    uid: str
    name: str
    start: date
    end: date
    federation: str
    location: str
    country: str
    link: str
    listing: str
    added: date | None  # when the source first listed it; None if the source doesn't say
    status: str | None = None
    lat: float | None = None
    lon: float | None = None
    state: str = ""
    continent: str = ""
    testing: str = "Unknown"
    equipment: list[str] = field(default_factory=list)
    events: list[str] = field(default_factory=list)


FEEDS: dict[str, Callable[[Meet], bool]] = {
    "all": lambda _: True,
    "au": lambda m: m.country == "Australia",
    "us": lambda m: m.country in {"USA", "United States"},
    # Russia and Belarus left out of the Europe feed on purpose
    "europe": lambda m: m.continent == "Europe" and m.country not in {"Russia", "Belarus"},
    "mx": lambda m: m.country == "Mexico",
    # "N entered" is only a roster count, so it doesn't prove sign-ups are open
    "open": lambda m: bool(m.status) and m.status.startswith("OPEN"),
}


def classify(name: str, organizers: list[str], categories: list[str]) -> tuple[str, list[str], list[str]]:
    """Testing policy, equipment and events from federation defaults, category tags and the meet name."""
    evidence = " | ".join([name, *categories])
    untested = bool(UNTESTED.search(evidence)) or any(o in UNTESTED_FEDS for o in organizers)
    tested = bool(TESTED.search(UNTESTED.sub("", evidence))) or any(o in TESTED_FEDS for o in organizers)
    testing = "Both" if tested and untested else "Tested" if tested else "Untested" if untested else "Unknown"

    equipment, events = set(), set()
    for cat in categories:
        if cat.casefold() == "classic raw" and "United States Powerlifting Association" in organizers:
            equipment.add("Wraps")  # USPA's Classic Raw division allows knee wraps
        elif hit := next((label for label, rx in EQUIPMENT if re.search(rx, cat, re.IGNORECASE)), None):
            equipment.add(hit)
    for text in [*categories, name]:
        events.update(label for label, rx in EVENTS if re.search(rx, text, re.IGNORECASE))
    # Single-lift meets often only say so in the name, e.g. "Bench for Boobs and Biceps" or "Torneo de Peso Muerto"
    if not events and not re.search(r"powerlift|squat|full", name, re.IGNORECASE):
        if re.search(r"bench|banca", name, re.IGNORECASE) and not re.search(r"deadlift|peso muerto", name, re.IGNORECASE):
            events.add("Bench only")
        elif re.search(r"deadlift|peso muerto", name, re.IGNORECASE) and not re.search(r"bench|banca", name, re.IGNORECASE):
            events.add("Deadlift only")
    return testing, sorted(equipment), sorted(events)


def get(url: str, **kwargs) -> requests.Response:
    time.sleep(0.2)
    # Uncached powerlifting.com pages take 10-30s+ to render
    response = SESSION.get(url, timeout=120, **kwargs)
    response.raise_for_status()
    return response


def powerlifting_com() -> list[Meet]:
    meets, page = [], 1
    while True:
        params = {"per_page": 50, "page": page, "start_date": date.today().isoformat()}
        data = get("https://powerlifting.com/wp-json/tribe/events/v1/events", params=params).json()
        for e in data["events"]:
            name = html.unescape(e["title"])
            organizers = [html.unescape(o["organizer"]) for o in e["organizer"]]
            if NOT_A_MEET.search(" ".join([name, *organizers])) and not LIFTS.search(name):
                continue
            venue = e["venue"] if isinstance(e["venue"], dict) else {}
            place = [venue.get(k) for k in ("venue", "city")]
            testing, equipment, events = classify(name, organizers, [html.unescape(c["name"]) for c in e["categories"]])
            meets.append(
                Meet(
                    uid=f"plcom-{e['id']}",
                    name=name,
                    start=date.fromisoformat(e["start_date"][:10]),
                    end=date.fromisoformat(e["end_date"][:10]),
                    federation=", ".join(o for o in organizers if not o.startswith("*")),
                    location=html.unescape(", ".join(p for p in place if p)),
                    state=html.unescape(venue.get("stateprovince") or ""),
                    country=venue.get("country") or "",
                    link=e["website"] or e["url"],
                    listing=e["url"],
                    added=date.fromisoformat(e["date"][:10]),
                    lat=float(venue["geo_lat"]) if venue.get("geo_lat") else None,
                    lon=float(venue["geo_lng"]) if venue.get("geo_lng") else None,
                    testing=testing,
                    equipment=equipment,
                    events=events,
                )
            )
        if page >= data["total_pages"]:
            return meets
        page += 1


def femepo() -> list[Meet]:
    # ponytail: single page of 300; follow nextPageToken if FEMEPO ever lists more
    url = "https://firestore.googleapis.com/v1/projects/femepo-web/databases/(default)/documents/events"
    meets = []
    for doc in get(url, params={"pageSize": 300}).json().get("documents", []):
        # Some records store the literal string "null" rather than a null value
        f = {k: val for k, v in doc["fields"].items() if (val := next(iter(v.values()))) not in (None, "", "null")}
        start = datetime.strptime(f["startDate"][:10].replace("-", "/"), "%Y/%m/%d").date()
        if start < date.today():
            continue
        state = f.get("state") or mx_state_in(f["title"])
        lat, lon = geocode(f.get("address"), f"{state}, Mexico" if state else None)
        testing, equipment, events = classify(f["title"], ["FEMEPO"], [])
        meets.append(
            Meet(
                uid=f"femepo-{doc['name'].rsplit('/', 1)[1]}",
                name=f["title"].strip(),
                start=start,
                end=start,
                federation="FEMEPO",
                location=f.get("address", ""),
                state=state,
                country="Mexico",
                link=f.get("url") or "https://femepo.com.mx/calendar.html",
                listing="https://femepo.com.mx/calendar.html",
                added=date.fromisoformat(doc["createTime"][:10]),
                lat=lat,
                lon=lon,
                testing=testing,
                equipment=equipment,
                events=events,
            )
        )
    return meets


LMP_URL = "https://www.lmp-powerlifting.com/competencias"
SPANISH_MONTHS = {m: i for i, m in enumerate(("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"), 1)}


def lmp_meet(e: dict) -> Meet:
    """One Liga Mexicana de Powerlifting event; its feed gives a registration deadline but no listing date."""
    d = e["fecha_evento"]
    start = date(int(d["año"]), SPANISH_MONTHS[fold(d["mes"])], int(d["dia"]))
    place = e.get("lugar") or {}
    deadline = date.fromisoformat(e["fecha_limite"]) if e.get("fecha_limite") else None
    status = None if deadline is None else f"OPEN · closes {deadline:%-d %b}" if deadline >= date.today() else "CLOSED"
    testing, equipment, events = classify(e["titulo"], ["Liga Mexicana de Powerlifting"], [])
    return Meet(
        uid=f"lmp-{e['id']}",
        name=e["titulo"].strip(),
        start=start,
        end=start,
        federation="Liga Mexicana de Powerlifting",
        location=(place.get("ciudad") or "").strip(),
        state=(place.get("estado") or "").strip(),
        country="Mexico",
        link=LMP_URL,
        listing=LMP_URL,
        added=None,
        status=status,
        testing=testing,
        equipment=equipment,
        events=events,
    )


def lmp() -> list[Meet]:
    meets = [lmp_meet(e) for e in get("https://www.lmp-powerlifting.com/api/eventos-activos").json()]
    meets = [m for m in meets if m.start >= date.today()]
    for m in meets:
        m.lat, m.lon = geocode(", ".join(p for p in (m.location, m.state, "Mexico") if p), f"{m.state}, Mexico" if m.state else None)
    return meets


def merge(base: list[Meet], extra: list[Meet]) -> list[Meet]:
    """Add meets from a secondary source unless the same meet (same country, within a day, similar name) is already listed."""
    # Same country is required: generic names like "... powerlifting meet" look similar to unrelated meets elsewhere
    names_by_day = defaultdict(list)
    for m in base:
        names_by_day[m.country, m.start].append(m.name.casefold())

    def known(m: Meet) -> bool:
        nearby = (n for offset in (-1, 0, 1) for n in names_by_day[m.country, m.start + timedelta(days=offset)])
        return any(difflib.SequenceMatcher(None, m.name.casefold(), n).ratio() > 0.55 for n in nearby)

    return base + [m for m in extra if not known(m)]


def fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().casefold()


def mx_state_in(title: str) -> str:
    # Longest names first so "Baja California Sur" wins over "Baja California"
    return next((state for name, state in sorted(MX_STATES.items(), key=lambda kv: -len(kv[0])) if re.search(rf"\b{fold(name)}\b", fold(title))), "")


def geocode(*queries: str | None) -> tuple[float | None, float | None]:
    """First OpenStreetMap hit in Mexico for the queries in order; the Mexican sources give addresses, not coordinates."""
    for query in filter(None, queries):
        time.sleep(1)  # Nominatim's usage policy: at most 1 request a second
        try:
            hits = get("https://nominatim.openstreetmap.org/search", params={"q": query, "format": "json", "limit": 1, "countrycodes": "mx"}).json()
        except requests.RequestException as err:
            logging.warning(f"Geocoding failed for {query!r}: {err}")
            continue
        if hits:
            return float(hits[0]["lat"]), float(hits[0]["lon"])
    return None, None


def flag(text: str) -> str | None:
    if re.search(r"SOLD OUT", text, re.IGNORECASE):
        return "SOLD OUT"
    if re.search(r"CANCELL?ED", text, re.IGNORECASE):
        return "CANCELLED"
    return None


def apa_status(page: str) -> str | None:
    title = re.search(r'<meta property="og:title" content="([^"]*)"', page)
    if title and (flagged := flag(title[1])):
        return flagged
    if "Registrations for this event are closed" in page:
        return "CLOSED"
    cap = re.search(r"Maximum number of entrants:</strong>\s*(\d+)", page)
    if ">Register<" in page:
        return f"OPEN · cap {cap[1]}" if cap else "OPEN"
    return None


def jotform_status(page: str) -> str | None:
    # A closed form drops the form markup and puts the director's message in <title>; anything else (an error page) proves nothing.
    if "jotform-form" not in page:
        title = re.search(r"<title>([^<]*)", page)
        message = title[1] if title else ""
        if re.search(r"sold out|\bfull\b", message, re.IGNORECASE):
            return "SOLD OUT"
        return "CLOSED" if re.search(r"closed|no longer|unavailable", message, re.IGNORECASE) else None
    cap = re.search(r"LIMITED TO (\d+)", page, re.IGNORECASE)
    return f"OPEN · cap {cap[1]}" if cap else "OPEN"


def liftingcast_status(docs: list[dict]) -> str:
    return f"{sum(d['_id'].startswith('l') for d in docs)} entered"


def status(meet: Meet) -> str | None:
    global liftingcast_blocked
    if flagged := flag(meet.name):
        return flagged
    url = meet.link.split("?")[0]
    host = urlparse(url).hostname or ""
    try:
        if host.endswith("powerliftingalliance.org.au"):
            return apa_status(get(url).text)
        if host.endswith("jotform.com"):
            return jotform_status(get(url).text)
        # Meets arrive soonest first, so the lookups LiftingCast allows go to the nearest meets.
        if host.endswith("liftingcast.com") and not liftingcast_blocked and (m := re.search(r"/meets/([a-z0-9]+)", url)):
            time.sleep(LIFTINGCAST_PAUSE)
            return liftingcast_status(get(f"https://liftingcast.com/api/meets/{m[1]}").json()["docs"])
    except requests.RequestException as err:
        if host.endswith("liftingcast.com") and err.response is not None and err.response.status_code == 429:
            liftingcast_blocked = True
        logging.warning(f"Status lookup failed for {url}: {err}")
    return None


def feed_ics(name: str, meets: list[Meet]) -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//powerlifting-meet-calendar//EN")
    cal.add("version", "2.0")
    cal.add("x-wr-calname", f"Powerlifting meets ({name})")
    stamp = datetime.now(UTC)
    for m in meets:
        event = Event()
        event.add("uid", f"{m.uid}@powerlifting-meet-calendar")
        event.add("summary", f"[{m.status}] {m.name}" if m.status else m.name)
        event.add("dtstart", m.start)
        event.add("dtend", m.end + timedelta(days=1))
        event.add("dtstamp", stamp)
        event.add("location", ", ".join(p for p in (m.location, m.state, m.country) if p))
        event.add("url", m.link)
        lines = [
            f"Federation: {m.federation or 'unknown'}",
            f"Testing: {m.testing}",
            f"Equipment: {', '.join(m.equipment) or 'not listed'}",
            f"Events: {', '.join(m.events) or 'not listed'}",
            f"Status: {m.status or 'unknown'}",
            f"Sign up / info: {m.link}",
        ]
        if m.listing != m.link:
            lines.append(f"Listing: {m.listing}")
        lines.append(f"Added: {m.added or 'not given'}")
        event.add("description", "\n".join(lines))
        cal.add_component(event)
    return cal.to_ical()


def assign_continents(meets: list[Meet]) -> None:
    names = sorted({m.country for m in meets} - {""})
    found = coco.convert(names=names, to="continent_7", not_found="Other")
    # coco returns a bare string, not a list, for a single name
    lookup = dict(zip(names, [found] if isinstance(found, str) else found, strict=True))
    for m in meets:
        m.continent = lookup.get(m.country, "Other")


def to_frame(meets: list[Meet]) -> pl.DataFrame:
    pulled_at = datetime.now(UTC)
    return pl.DataFrame([asdict(m) | {"pulled_at": pulled_at} for m in meets])


def main() -> None:
    meets = merge(powerlifting_com() + femepo(), lmp())
    # An empty or truncated pull would wipe the dashboard and subscribed calendars; fail and keep the last landing instead.
    if len(meets) < 200:
        raise SystemExit(f"Only {len(meets)} meets pulled; not landing")
    for m in meets:
        # Keep a status the source already gave (LMP's deadline) unless the sign-up page says more
        m.status = status(m) or m.status
    assign_continents(meets)

    common_io.io_write_from_local_to_s3(to_frame(meets), conf.meets_local, conf.meets_s3_key)

    s3 = boto3.client("s3")
    for name, keep in FEEDS.items():
        selected = [m for m in meets if keep(m)]
        key = conf.create_output_file_path(conf.OutputPathType.LANDING, conf.FileLocation.S3, file_name=f"meets-{name}.ics")
        s3.put_object(Bucket=conf.bucket_name, Key=key, Body=feed_ics(name, selected), ContentType="text/calendar; charset=utf-8")
        logging.info(f"Uploaded {len(selected)} meets to s3://{conf.bucket_name}/{key}")


if __name__ == "__main__":
    main()
