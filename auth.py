"""
auth.py — Authentication & token validation for Paytm Money API
"""

import requests
from typing import Dict, Any, Tuple


PAYTM_BASE_URL = "https://developer.paytmmoney.com"


def authenticate(api_key: str, api_secret: str, request_token: str) -> Tuple[Dict[str, Any] | None, str | None]:
    """
    Exchange request_token for token bundle via Paytm Money OAuth.
    Returns: (token_payload, error_message)
    """
    try:
        url = f"{PAYTM_BASE_URL}/accounts/v2/gettoken"
        payload = {
            "api_key": api_key,
            "api_secret_key": api_secret,
            "request_token": request_token,
        }
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", body) if isinstance(body, dict) else {}

        access_token = data.get("access_token") or body.get("access_token")
        if not access_token:
            return None, "Access token not found in response."

        return {
            "access_token": access_token,
            "public_access_token": data.get("public_access_token") or body.get("public_access_token") or "",
            "read_access_token": data.get("read_access_token") or body.get("read_access_token") or "",
            "raw_response": body,
        }, None

    except requests.exceptions.ConnectionError:
        return None, "Connection error. Check internet."
    except requests.exceptions.Timeout:
        return None, "Request timed out."
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP error: {e.response.status_code}"
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"


def validate_token(token: str) -> Tuple[bool, str]:
    """
    Validate token by making a lightweight API call.
    In demo mode (token == 'DEMO'), always returns True.
    Returns: (is_valid, message)
    """
    if token == "DEMO":
        return True, "Demo mode activated."

    # Lightweight validation — try hitting the user profile endpoint
    try:
        url = f"{PAYTM_BASE_URL}/accounts/v2/profile"
        headers = {"x-jwt-token": token, "Content-Type": "application/json"}
        resp = requests.get(url, headers=headers, timeout=8)

        if resp.status_code == 200:
            return True, "Token valid."
        elif resp.status_code == 401:
            return False, "Token expired or invalid. Please re-authenticate."
        elif resp.status_code == 403:
            return False, "Access forbidden. Check API permissions."
        else:
            # Non-fatal — assume token might still work for option chain
            return True, f"Warning: status {resp.status_code}, proceeding anyway."

    except requests.exceptions.ConnectionError:
        return False, "Cannot connect to Paytm Money servers."
    except requests.exceptions.Timeout:
        return False, "Validation timed out."
    except Exception as e:
        # Fallback: accept token, let actual API call fail with proper error
        return True, f"Could not validate (will try anyway): {str(e)}"
