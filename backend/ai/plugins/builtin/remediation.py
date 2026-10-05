from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session

from ai.plugins.base import BasePlugin


class RemediationPlugin(BasePlugin):
    """
    Provides standard operating procedures (SOPs), triage runbooks,
    and incident ticket generation for NOC engineers resolving Milano telecom events.
    """

    @property
    def name(self) -> str:
        return "remediation_runbooks"

    @property
    def display_name(self) -> str:
        return "Remediation & Runbooks"

    @property
    def description(self) -> str:
        return (
            "Suggests standard operating procedures (SOPs), diagnostic runbooks, "
            "and structured incident tickets for anomalous grid events."
        )

    def get_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": "suggest_remediation_sop",
                "description": (
                    "Retrieve the standard NOC remediation SOP (Standard Operating Procedure) "
                    "for a diagnosed network issue (e.g., severe traffic drop, hotspot surge, data lag)."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "issue_type": {
                            "type": "string",
                            "enum": ["traffic_drop", "hotspot_surge", "pipeline_lag", "general_anomaly"],
                            "description": "Category of the network issue.",
                        },
                        "severity": {
                            "type": "string",
                            "enum": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                            "description": "Assessed severity level.",
                        },
                    },
                    "required": ["issue_type", "severity"],
                },
            },
            {
                "name": "draft_incident_ticket",
                "description": (
                    "Draft a structured incident ticket for NOC escalation or handoff."
                ),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "grid_id": {
                            "type": "integer",
                            "description": "Grid ID affected.",
                        },
                        "severity": {
                            "type": "string",
                            "enum": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                        },
                        "summary": {
                            "type": "string",
                            "description": "Concise summary of the incident.",
                        },
                        "recommended_action": {
                            "type": "string",
                            "description": "Recommended next immediate step for field/L2 engineers.",
                        },
                    },
                    "required": ["grid_id", "severity", "summary", "recommended_action"],
                },
            },
        ]

    def execute_tool(
        self,
        name: str,
        tool_input: Dict[str, Any],
        db: Session,
        context: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if name == "suggest_remediation_sop":
            issue_type = tool_input.get("issue_type", "general_anomaly")
            severity = tool_input.get("severity", "MEDIUM")

            sops = {
                "traffic_drop": [
                    "1. Confirm pipeline freshness via Data Quality plugin to eliminate data ingest delay.",
                    "2. Check radio access network (RAN) power alarms on cell sites covering this grid.",
                    "3. Inspect adjacent neighbor grids (N-1, N+1) to determine if failure is isolated or regional.",
                    "4. If drop_probability > 0.85, dispatch local field maintenance team for hardware check.",
                ],
                "hotspot_surge": [
                    "1. Verify if an event (e.g. stadium match, concert, public demonstration) is occurring in this grid.",
                    "2. Check for signaling channel congestion or packet drops on the backhaul link.",
                    "3. Initiate dynamic load balancing or tilt antenna parameters to offload to neighboring underutilized cells.",
                    "4. Monitor handover success rates across the sector.",
                ],
                "pipeline_lag": [
                    "1. Check Airflow DAG execution logs and Spark streaming job health.",
                    "2. Verify Kafka broker ingestion offsets and MySQL database connection pool.",
                    "3. Trigger manual DAG backfill for the delayed time interval once upstream ingestion restores.",
                ],
                "general_anomaly": [
                    "1. Collect 24-hour activity trend comparing current metrics to historical baseline.",
                    "2. Verify SMS, voice, and internet split to isolate service-specific degradation.",
                    "3. Escalate to L2 Operations if anomaly persists over 2 consecutive reporting hours.",
                ],
            }

            return {
                "issue_type": issue_type,
                "severity": severity,
                "sop_steps": sops.get(issue_type, sops["general_anomaly"]),
                "escalation_contact": "NOC-Tier2-Radio@milano-telecom.it",
            }

        if name == "draft_incident_ticket":
            import uuid
            ticket_id = f"INC-MIL-{uuid.uuid4().hex[:6].upper()}"
            return {
                "ticket_id": ticket_id,
                "grid_id": tool_input["grid_id"],
                "severity": tool_input["severity"],
                "summary": tool_input["summary"],
                "recommended_action": tool_input["recommended_action"],
                "status": "DRAFTED",
                "assigned_queue": "Milano Field Operations & RAN Tier-2",
            }

        raise ValueError(f"RemediationPlugin received unknown tool: {name}")

    def get_prompt_contribution(self) -> Optional[str]:
        return (
            "- When recommending NEXT CHECKS, use specific actionable steps from the remediation runbook.\n"
            "- If the operator requests an incident escalation, offer to draft an incident ticket."
        )
