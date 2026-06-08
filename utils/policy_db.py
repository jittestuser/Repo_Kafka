"""
Mock Policy Database
--------------------
Simulates a backend database that maps PolicyNumber → PolicyName + Postcode.
Replace with a real DB / API call in production.
"""

POLICY_DATABASE = {
    "POL-1001": {"policy_name": "Comprehensive Health Cover",    "postcode": "EC1A 1BB"},
    "POL-1002": {"policy_name": "Basic Life Assurance",          "postcode": "W1A 0AX"},
    "POL-1003": {"policy_name": "Home & Contents Premium",       "postcode": "SW1A 2AA"},
    "POL-1004": {"policy_name": "Motor Vehicle Full Cover",      "postcode": "E1 6AN"},
    "POL-1005": {"policy_name": "Travel Worldwide Protect",      "postcode": "N1 9GU"},
    "POL-1006": {"policy_name": "Critical Illness Shield",       "postcode": "SE1 7PB"},
    "POL-1007": {"policy_name": "Income Protection Plus",        "postcode": "WC2N 5DU"},
    "POL-1008": {"policy_name": "Business Liability Guard",      "postcode": "EC2V 8RF"},
    "POL-1009": {"policy_name": "Pet Care Essential",            "postcode": "NW1 4NP"},
    "POL-1010": {"policy_name": "Landlord Property Shield",      "postcode": "BS1 4DJ"},
}


def lookup_policy(policy_number: str) -> dict | None:
    """Return PolicyName and Postcode for a given PolicyNumber, or None if not found."""
    return POLICY_DATABASE.get(policy_number)
