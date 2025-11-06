import os
import json
import re
import requests
from datetime import datetime, timezone

import azure.functions as func
from azure.functions import FunctionApp
from msal import ConfidentialClientApplication

app = FunctionApp()


def month_bounds_utc(now_utc):
    """Return (start_iso_z, end_iso_z) for the current calendar month in UTC with Z suffix."""
    start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_cell(val, na="NA"):
    """Normalize for tab output: empty -> NA; ISO date -> YYYY-MM-DD; bool -> Yes/No."""
    if val is None or val == "":
        return na
    if isinstance(val, bool):
        return "Yes" if val else "No"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val)
    # Try to normalize ISO timestamps to YYYY-MM-DD
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        pass
    return s


def get_value(obj, field):
    """Prefer formatted annotation if present, else raw."""
    if not obj:
        return ""
    fmt_key = f"{field}@OData.Community.Display.V1.FormattedValue"
    if fmt_key in obj and obj[fmt_key] not in (None, ""):
        return obj[fmt_key]
    return obj.get(field, "")


def sanitize_guid(g):
    """Return GUID in canonical form without braces/spaces if present."""
    if not g:
        return ""
    s = str(g).strip().strip("{}").strip()
    m = re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s)
    return m.group(0) if m else s


def normalize_multichoice(value: str) -> str:
    """
    Dataverse formats multi-select choice labels with semicolons like:
      'A; B; C'
    Convert to:
      'A, B, C'
    """
    if not value:
        return value
    # Split on ';', trim parts, drop empties, join with ', '
    parts = [p.strip() for p in str(value).split(";")]
    parts = [p for p in parts if p]
    return ", ".join(parts)


@app.function_name(name="GetMDRATE")
@app.route(route="getmdrate", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_mdrate(req: func.HttpRequest) -> func.HttpResponse:
    # --- Config / Auth ---
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

    # --- Date filter for current month (UTC) ---
    start_z, end_z = month_bounds_utc(datetime.now(timezone.utc))

    # --- Admissions request with $expand=cp_Client (capital C) ---
    admissions_select = ",".join([
        # admissions fields (include cp_pseudoname here)
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
        # contact fields ONLY
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
        "$expand": f"cp_Client($select={contact_select})"  # navigation property name usually cp_Client
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

    # --- Substance lookup resolver (still separate call) ---
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

    # --- Build rows ---
    medical_rows = []
    social_rows = []

    for rec in admissions:
        detoxtype = rec.get("cp_detoxtype")
        med_disc = rec.get("cp_medicaldischargedate")
        is_med = (detoxtype == 121570000) or (detoxtype == 121570001 and med_disc is not None)
        is_soc = (detoxtype == 121570001)
        if not (is_med or is_soc):
            continue

        contact = rec.get("cp_Client") or {}  # from $expand
        opioid_lookup_id = rec.get("_cp_opioidagonisttherapy_value")
        opioid_name = fetch_substance_name(opioid_lookup_id)

        # Pull formatted values when available, then normalize multi-choice separators
        contributing = normalize_multichoice(get_value(rec, "cp_contributingfactors"))
        post_referrals = normalize_multichoice(get_value(rec, "cp_postdischargereferral"))

        vals = [
            get_value(contact, "cp_ahcnumber"),                 # 1 (Contact)
            get_value(contact, "cp_clientoutofprovince"),       # 2 (Contact)
            get_value(rec, "cp_pseudoname"),                    # 3 (Admissions)
            get_value(rec, "cp_servicerequestdate"),            # 4 (Admissions)
            get_value(rec, "cp_admissiondate"),                 # 5 (Admissions)
            get_value(rec, "cp_actualdischargedate"),           # 6 (Admissions)
            get_value(contact, "address1_postalcode"),          # 7 (Contact)
            get_value(contact, "cp_gender"),                    # 8 (Contact)
            get_value(contact, "cp_age"),                       # 9 (Contact)
            get_value(rec, "cp_primarysubstanceused"),          # 10 (Admissions)
            get_value(rec, "cp_othersubstances"),               # 11 (Admissions)
            contributing,                                       # 12 (Admissions multi-choice normalized)
            get_value(rec, "cp_incomesource"),                  # 13 (Admissions)
            opioid_name,                                        # 14 (from cp_substances)
            get_value(rec, "cp_reasonfordischargemdrate"),      # 15 (Admissions)
            get_value(rec, "cp_reasonforhospitaladmissionmdrate"),  # 16 (Admissions)
            post_referrals,                                     # 17 (Admissions multi-choice normalized)
            get_value(contact, "cp_mrpnumber")                  # 18 (Contact)
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
    social_report = header + "\n " + ("".join(social_rows) if social_rows else "")

    body = {"medical_report": medical_report, "social_report": social_report}
    return func.HttpResponse(json.dumps(body), mimetype="application/json")
