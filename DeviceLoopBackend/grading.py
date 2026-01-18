# DeviceLoopBackend/grading.py
from __future__ import annotations

from decimal import Decimal
from typing import Dict, Any


class GradeRejected(Exception):
    """
    Raised when the device should not be accepted on the platform at all
    (for example: cannot power on, water damage, jailbroken, etc.).
    """
    pass


def _to_lower(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


def _num(value: Any) -> Decimal:
    """
    Helper to safely turn Dynamo values into Decimal.
    DynamoDB may give us int, float, Decimal or even string.
    """
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def compute_initial_grade_and_range(
    device: Dict[str, Any],
    questionnaire: Dict[str, Any],
) -> tuple[str, float, float]:
    

    # ---- 1) Hard rejections -------------------------------------------------
    free_of_locks = bool(questionnaire.get("freeOfLocks", False))
    can_power_on = bool(questionnaire.get("canPowerOn", False))
    serious_issues = questionnaire.get("seriousIssues") or []

    # Anything here means we *reject* the device
    if not free_of_locks:
        raise GradeRejected("Device must be free of passcodes / remote locks.")
    if not can_power_on:
        raise GradeRejected("Device cannot power on.")
    if serious_issues:
        # you can customise the message if you want
        raise GradeRejected("Device has serious issues and cannot be listed.")

    # ---- 2) Soft issues that degrade the grade ------------------------------
    screen = _to_lower(questionnaire.get("screenCondition"))
    body = _to_lower(questionnaire.get("bodyCondition"))
    cameras = _to_lower(questionnaire.get("cameras"))
    core = _to_lower(questionnaire.get("coreFunctions"))
    biometric = _to_lower(questionnaire.get("biometric"))

    downgrade = 0  # 0 = A, 1 = B, 2 = C

    def minor(txt: str) -> bool:
        return any(w in txt for w in ["2-3 minor", "2-3 minor", "some minor", "some issues"])

    def heavy(txt: str) -> bool:
        return any(w in txt for w in ["heavy", "cracked", "dented", "major"])

    # screen
    if minor(screen):
        downgrade = max(downgrade, 1)
    if heavy(screen):
        downgrade = max(downgrade, 2)

    # body
    if minor(body):
        downgrade = max(downgrade, 1)
    if heavy(body):
        downgrade = max(downgrade, 2)

    # cameras
    if cameras and cameras not in ("ok", "both_ok", "both are fine"):
        downgrade = max(downgrade, 1)

    # core functions (speaker, Wi-Fi, etc.)
    if core and core not in ("ok", "yes, everything is working"):
        downgrade = max(downgrade, 1)

    # biometric
    if biometric and biometric not in ("yes", "working"):
        downgrade = max(downgrade, 1)

    # Final grade
    if downgrade <= 0:
        grade = "A"
        min_key, max_key = "Grade_A_MIN", "Grade_A_MAX"
    elif downgrade == 1:
        grade = "B"
        min_key, max_key = "Grade_B_MIN", "Grade_B_MAX"
    else:
        grade = "C"
        min_key, max_key = "Grade_C_MIN", "Grade_C_MAX"

    min_price = _num(device.get(min_key))
    max_price = _num(device.get(max_key))

    # return grade + floats for JSON
    return grade, float(min_price), float(max_price)
