from typing import Any, Dict
from ai.hooks.base import BaseHook


class NocFormatComplianceHook(BaseHook):
    """
    Checks that NOC incident investigation responses adhere to the standard
    SEVERITY, EVIDENCE, INTERPRETATION, and NEXT CHECKS schema expected by
    Milano operations. Records compliance status in context for frontend telemetry.
    """

    @property
    def name(self) -> str:
        return "format_compliance"

    @property
    def priority(self) -> int:
        return 90

    def post_run(
        self,
        response_text: str,
        context: Dict[str, Any],
    ) -> str:
        upper = response_text.upper()

        has_severity = "SEVERITY" in upper
        has_evidence = "EVIDENCE" in upper
        has_interpretation = "INTERPRETATION" in upper
        has_next_checks = "NEXT CHECKS" in upper or "NEXT STEPS" in upper

        # Only enforce compliance scoring if response appears to be an investigation
        is_investigation = any([has_severity, has_evidence, has_interpretation])
        if is_investigation:
            compliance = has_severity and has_evidence and has_interpretation
            context["compliance_passed"] = compliance
            context["compliance_details"] = {
                "has_severity": has_severity,
                "has_evidence": has_evidence,
                "has_interpretation": has_interpretation,
                "has_next_checks": has_next_checks,
            }
        else:
            # Casual conversation, general summaries or greetings are considered compliant
            context["compliance_passed"] = True
            context["compliance_details"] = {"general_query": True}

        return response_text
