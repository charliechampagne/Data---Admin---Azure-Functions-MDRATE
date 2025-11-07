import os
import json
import re
import requests
from datetime import datetime, timezone, tzinfo, timedelta

import azure.functions as func
from azure.functions import FunctionApp
from msal import ConfidentialClientApplication

# ---- Robust Calgary timezone loader ----
def _load_calgary_tz() -> tzinfo:
    # Try stdlib zoneinfo with tzdata
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+
        return ZoneInfo("America/Edmonton")
    except Exception:
        pass
    # Try python-dateutil if available
    try:
        from dateutil.tz import gettz
        tz = gettz("America/Edmonton")
        if tz is not None:
            return tz
    except Exception:
        pass
    # Last-resort fixed offset (no DST). Better than crashing.
    class _FixedOffset(tzinfo):
        def __init__(self, minutes):
            self._offset = timedelta(minutes=minutes)
        def utcoffset(self, dt): return self._offset
        def tzname(self, dt):    return "MST"
        def dst(self, dt):       return timedelta(0)
    return _FixedOffset(-7 * 60)

CALGARY_TZ = _load_calgary_tz()

app = FunctionApp()

# ----------------- Helpers -----------------

def last_month_bounds_utc(now_utc: datetime):
    """
    Return UTC (Z) bounds for previous calendar month:
    [start_of_last_month, start_of_this_month)
    """
    this_month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if this_month_start.month == 1:
        last_start = this_month_start.replace(year=this_month_start.year - 1, month=12)
    else:
        last_start = this_month_start.replace(month=this_month_start.month - 1)
    last_end = this_month_start
    return last_start.strftime("%Y-%m-%dT%H:%M:%SZ"), last_end.strftime("%Y-%m-%dT%H:%M:%SZ")

def fmt_cell(val, na="NA"):
    if val is None or val == "":
        return na
    if isinstance(val, bool):
        return "Yes" if val else "No"
    if isinstance(val, (int, float)):
        return str(val)
    return str(val)

def get_value(obj, field):
    if not obj:
        return ""
    fmt_key = f"{field}@OData.Community.Display.V1.FormattedValue"
    if fmt_key in obj and obj[fmt_key] not in (None, ""):
        return obj[fmt_key]
    return obj.get(field, "")

def sanitize_guid(g):
    if not g:
        return ""
    s = str(g).strip().strip("{}").strip()
    m = re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s)
    return m.group(0) if m else s

def normalize_multichoice(value: str) -> str:
    if not value:
        return value
    parts = [p.strip() for p in str(value).split(";")]
    parts = [p for p in parts if p]
    return ", ".join(parts)

def utc_to_calgary_str(dt_val) -> str:
    """
    Convert OData DateTimeOffset (UTC) to Calgary local 'MM/DD/YYYY HH:MM AM/PM'.
    Accepts ISO string or datetime.
    """
    if not dt_val:
        return "NA"
    # Parse input
    if isinstance(dt_val, str):
        s = dt_val.strip()
        try:
            if s.endswith("Z"):
                dt_utc = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                parsed = datetime.fromisoformat(s)
                dt_utc = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return s
    elif isinstance(dt_val, datetime):
        dt_utc = dt_val if dt_val.tzinfo else dt_val.replace(tzinfo=timezone.utc)
    else:
        return str(dt_val)
    # Convert
    local_dt = dt_utc.astimezone(CALGARY_TZ)
    return local_dt.strftime("%m/%d/%Y %I:%M %p")

# ----------------- Function -----------------

@app.function_name(name="GetMDRATE")
@app.route(route="getmdrate", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_mdrate(req: func.HttpRequest) -> func.HttpResponse:
    # Config / Auth
    try:
        tenant_id = os.environ["TENANT_ID"]
        client_id = os.environ["CLIENT_ID"]
        client_secret = os.environ["CLIENT_SECRET"]
        dataverse_url = os.environ["DATAVERSE_URL"].rstrip("/")
    except KeyError as ke:
        return func.HttpResponse(f"Missing environment variable: {ke}", status_code=500)

    cca = ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret
    )
    token_result = cca.acquire_token_for_client(scopes=[f"{dataverse_url}/.default"])
    if "access_token" not in token_result:
        return func.HttpResponse(
            f"Auth error: {token_result.get('error_description','Failed to acquire token')}",
            status_code=500
        )
    token = token_result["access_token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Prefer": 'odata.include-annotations="OData.Community.Display.V1.FormattedValue"'
    }

    # Last month filter (UTC Z)
    start_z, end_z = last_month_bounds_utc(datetime.now(timezone.utc))

    # Admissions with $expand=cp_Client
    admissions_select = ",".join([
        "cp_servicerequestdate",
        "cp_admissiondate",
        "cp_actualdischargedate",
        "cp_primarysubstanceused",
        "cp_othersubstances",
        "cp_contributingfactors",
        "cp_incomesource",
        "cp_reasonfordischargemdrate",
        "cp_reasonforhospitaladmissionmdrate",
        "cp_postdischargereferral",
        "cp_detoxtype",
        "cp_medicaldischargedate",
        "cp_pseudoname",
        "_cp_opioidagonisttherapy_value"
    ])
    contact_select = ",".join([
        "cp_ahcnumber",
        "cp_clientoutofprovince",
        "address1_postalcode",
        "cp_gender",
        "cp_age",
        "cp_mrpnumber"
    ])
    filter_str = f"cp_actualdischargedate ge {start_z} and cp_actualdischargedate lt {end_z}"

    admissions_url = f"{dataverse_url}/api/data/v9.2/cp_cp_admissions"
    params = {
        "$select": admissions_select,
        "$filter": filter_str,
        "$expand": f"cp_Client($select={contact_select})"
    }

    try:
        resp = requests.get(admissions_url, headers=headers, params=params)
    except Exception as e:
        return func.HttpResponse(f"Dataverse query error: {e}", status_code=500)

    if resp.status_code != 200:
        return func.HttpResponse(f"Dataverse query failed: {resp.text}", status_code=500)

    try:
        admissions = resp.json().get("value", [])
    except Exception:
        return func.HttpResponse("Failed to parse admissions JSON.", status_code=500)

    # Substance name resolver
    def fetch_substance_name(substance_guid):
        guid = sanitize_guid(substance_guid)
        if not guid:
            return ""
        list_url = f"{dataverse_url}/api/data/v9.2/cp_substances"
        r = requests.get(
            list_url,
            headers=headers,
            params={"$select": "cp_nameofsubstance", "$filter": f"cp_substanceid eq {guid}", "$top": "1"}
        )
        if r.status_code == 200:
            try:
                items = r.json().get("value", [])
                return items[0].get("cp_nameofsubstance", "") if items else ""
            except Exception:
                return ""
        return ""

    # Build rows
    medical_rows, social_rows = [], []

    for rec in admissions:
        detoxtype = rec.get("cp_detoxtype")
        med_disc = rec.get("cp_medicaldischargedate")
        is_med = (detoxtype == 121570000) or (detoxtype == 121570001 and med_disc is not None)
        is_soc = (detoxtype == 121570001) and (med_disc is None)
        if not (is_med or is_soc):
            continue

        contact = rec.get("cp_Client") or {}
        opioid_lookup_id = rec.get("_cp_opioidagonisttherapy_value")
        opioid_name = fetch_substance_name(opioid_lookup_id)

        # Normalize multi-choice labels
        contributing = normalize_multichoice(get_value(rec, "cp_contributingfactors"))
        post_referrals = normalize_multichoice(get_value(rec, "cp_postdischargereferral"))

        # Use raw datetime values -> convert to Calgary local
        srd_raw  = rec.get("cp_servicerequestdate")
        adm_raw  = rec.get("cp_admissiondate")
        disc_raw = rec.get("cp_actualdischargedate")

        vals = [
            get_value(contact, "cp_ahcnumber"),
            get_value(contact, "cp_clientoutofprovince"),
            get_value(rec, "cp_pseudoname"),
            utc_to_calgary_str(srd_raw),
            utc_to_calgary_str(adm_raw),
            utc_to_calgary_str(disc_raw),
            get_value(contact, "address1_postalcode"),
            get_value(contact, "cp_gender"),
            get_value(contact, "cp_age"),
            get_value(rec, "cp_primarysubstanceused"),
            get_value(rec, "cp_othersubstances"),
            contributing,
            get_value(rec, "cp_incomesource"),
            opioid_name,
            get_value(rec, "cp_reasonfordischargemdrate"),
            get_value(rec, "cp_reasonforhospitaladmissionmdrate"),
            post_referrals,
            get_value(contact, "cp_mrpnumber")
        ]
        line = "\t".join(fmt_cell(v) for v in vals) + "\n "
        if is_med:
            medical_rows.append(line)
        if is_soc:
            social_rows.append(line)

    header = (
        "Personal Health Number\tOut of Province\tPseudo Name\tService Request Date\tAdmission Date\t"
        "Discharge Date\tPostal Code\tGender\tAge in Years\tPrimary Substance\tOther Substances\t"
        "Contributing Factors\tIncome Source\tOpioid Agonist Therapy\tReason for Discharge\t"
        "Reason for Hospital Admission\tPost-discharge Referrals\tMRP Client ID (for sites using MRP)"
    )

    medical_report = header + "\n " + ("".join(medical_rows) if medical_rows else "")
    social_report  = header + "\n " + ("".join(social_rows)  if social_rows  else "")

    return func.HttpResponse(
        json.dumps({"medical_report": medical_report, "social_report": social_report}),
        mimetype="application/json"
    )
