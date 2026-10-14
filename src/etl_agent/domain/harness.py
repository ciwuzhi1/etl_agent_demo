from enum import StrEnum


class HarnessRole(StrEnum):
    """描述 V2 职责分离角色；M2 会把项目成员关系映射为这些能力。"""

    DATA_ENGINEER = "DATA_ENGINEER"
    DATA_APPROVER = "DATA_APPROVER"
    SECURITY_APPROVER = "SECURITY_APPROVER"
    OPERATOR = "OPERATOR"
    AUDITOR = "AUDITOR"
