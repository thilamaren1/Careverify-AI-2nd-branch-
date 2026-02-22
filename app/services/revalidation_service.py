"""
CareVerify - Revalidation Service
Coordinates the end-to-end re-scoring pipeline after document arrival.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from ai.models.ensemble_engine import get_ai_engine
from ai.pipeline.feature_engineering import FeatureEngineer

from app.services.supabase_client import get_supabase_admin
from app.services.extraction_service import get_extraction_service
from app.services.audit_service import AuditService
from app.tasks.ai_tasks import analyze_claim_async

logger = logging.getLogger(__name__)


def _map_recommendation_to_status(recommendation: str) -> str:
    return {
        "AUTO_APPROVE": "pending_review",
        "APPROVE_WITH_REVIEW": "pending_review",
        "COMPLIANCE_REVIEW_REQUIRED": "compliance_review",
        "HIGH_RISK_HOLD": "compliance_review",
    }.get(recommendation, "pending_review")


def _build_reviewer_suggestion(recommendation: str, detected_risks: List[str]) -> str:
    if recommendation == "AUTO_APPROVE":
        return "Low-risk claim profile. Approve unless manual discrepancy is identified."
    if "MISSING_AUTHORIZATION" in detected_risks:
        return "Authorization evidence missing. Request prior authorization document before approval."
    if recommendation == "HIGH_RISK_HOLD":
        return "High-risk hold recommended. Escalate to compliance specialist for manual review."
    return "Proceed with targeted compliance review on extracted risk indicators."


class RevalidationService:
    """
    Orchestrates the intelligent claim revalidation sequence.
    """

    @staticmethod
    def revalidate_claim(claim_id: str, document_id: str = None) -> Dict[str, Any]:
        """
        Synchronous revalidation for immediate API feedback.
        Also schedules a background full analysis task for eventual consistency.
        """
        logger.info(f"[Revalidation] Starting intelligent cycle for claim {claim_id}")
        supabase = get_supabase_admin()
        extractor = get_extraction_service()
        engine = get_ai_engine()

        claim = supabase.table("claims").select("*").eq("id", claim_id).single().execute().data
        if not claim:
            return {"status": "error", "message": "Claim not found", "claim_id": claim_id}

        docs = (
            supabase.table("claim_documents")
            .select("id, ocr_text, ocr_data, file_name, created_at")
            .eq("claim_id", claim_id)
            .order("created_at", desc=False)
            .execute()
            .data
        )
        if not docs:
            return {"status": "error", "message": "No documents found for claim", "claim_id": claim_id}

        combined_text_parts: List[str] = []
        combined_ocr_data: Dict[str, Any] = {}
        for doc in docs:
            if doc.get("ocr_text"):
                combined_text_parts.append(str(doc.get("ocr_text")))
            if isinstance(doc.get("ocr_data"), dict):
                combined_ocr_data.update(doc["ocr_data"])
                combined_text_parts.append(str(doc["ocr_data"]))

        combined_text = "\n\n".join(combined_text_parts).strip()
        facts = extractor.extract(combined_text)
        logger.info(f"[Revalidation] Facts extracted: {facts.summary}")

        feature_engineer = FeatureEngineer(supabase)
        features = feature_engineer.build_features(claim, combined_ocr_data)
        ensemble_result = engine.analyze(features, combined_text)

        violations = sorted(
            set(
                facts.detected_risks
                + [
                    risk.get("factor", "UNKNOWN_RISK").upper().replace(" ", "_")
                    for risk in ensemble_result.risk_factors
                    if risk.get("impact", 0) < 0
                ]
            )
        )
        workflow_stage = (
            "auto_approval_review"
            if ensemble_result.recommendation == "AUTO_APPROVE"
            else "compliance_review"
        )
        auto_approval_eligible = bool(
            ensemble_result.recommendation == "AUTO_APPROVE"
            and "MISSING_AUTHORIZATION" not in facts.detected_risks
            and "MISSING_PROVIDER_IDENTIFIER" not in facts.detected_risks
        )
        reviewer_suggestion = _build_reviewer_suggestion(ensemble_result.recommendation, facts.detected_risks)
        next_status = _map_recommendation_to_status(ensemble_result.recommendation)
        if claim.get("status") == "draft":
            next_status = "draft"
            workflow_stage = "pre_submission_revalidation"

        ai_explanation = claim.get("ai_explanation") or {}
        ai_explanation.update(
            {
                "extracted_medical_facts": facts.to_dict(),
                "matched_policies": facts.matched_policies,
                "detected_risks": facts.detected_risks,
                "violation_flags": violations,
                "reviewer_suggestion": reviewer_suggestion,
                "workflow_stage": workflow_stage,
                "auto_approval_eligible": auto_approval_eligible,
                "explanation_text": ensemble_result.explanation_text,
                "risk_factors": ensemble_result.risk_factors,
                "source_document_id": document_id,
                "last_revalidated_at": datetime.utcnow().isoformat(),
            }
        )

        update_data = {
            "diagnosis_codes": facts.diagnosis_codes or claim.get("diagnosis_codes", []),
            "procedure_codes": facts.procedure_codes or claim.get("procedure_codes", []),
            "trust_score": ensemble_result.trust_score,
            "fraud_probability": ensemble_result.fraud_probability,
            "anomaly_score": ensemble_result.anomaly_score,
            "approval_likelihood": ensemble_result.approval_likelihood,
            "ai_recommendation": ensemble_result.recommendation,
            "ai_analyzed_at": datetime.utcnow().isoformat(),
            "status": next_status,
            "ai_explanation": ai_explanation,
            "violation_flags": violations,
            "ai_confidence_score": ensemble_result.confidence,
            "extracted_medical_facts": facts.to_dict(),
            "matched_policies": facts.matched_policies,
            "detected_risks": facts.detected_risks,
            "reviewer_suggestion": reviewer_suggestion,
            "workflow_stage": workflow_stage,
            "auto_approval_eligible": auto_approval_eligible,
        }
        basic_update_data = {
            "diagnosis_codes": update_data["diagnosis_codes"],
            "procedure_codes": update_data["procedure_codes"],
            "trust_score": update_data["trust_score"],
            "fraud_probability": update_data["fraud_probability"],
            "anomaly_score": update_data["anomaly_score"],
            "approval_likelihood": update_data["approval_likelihood"],
            "ai_recommendation": update_data["ai_recommendation"],
            "ai_analyzed_at": update_data["ai_analyzed_at"],
            "status": update_data["status"],
            "ai_explanation": update_data["ai_explanation"],
        }
        try:
            supabase.table("claims").update(update_data).eq("id", claim_id).execute()
        except Exception as exc:
            logger.warning(
                f"[Revalidation] Extended claim intelligence fields unavailable; "
                f"falling back to base fields for claim {claim_id}: {exc}"
            )
            supabase.table("claims").update(basic_update_data).eq("id", claim_id).execute()

        ai_record = {
            "claim_id": claim_id,
            "model_version": ensemble_result.model_version,
            "xgboost_fraud_score": ensemble_result.xgboost_fraud_score,
            "rf_approval_score": ensemble_result.rf_approval_score,
            "isolation_anomaly_score": ensemble_result.isolation_anomaly_score,
            "autoencoder_anomaly_score": ensemble_result.autoencoder_anomaly_score,
            "nlp_sentiment_score": ensemble_result.nlp_sentiment_score,
            "nlp_entities": ensemble_result.nlp_entities,
            "trust_score": ensemble_result.trust_score,
            "final_recommendation": ensemble_result.recommendation,
            "confidence": ensemble_result.confidence,
            "feature_importances": ensemble_result.feature_importances,
            "shap_values": ensemble_result.shap_values,
            "explanation_text": ensemble_result.explanation_text,
            "risk_factors": ensemble_result.risk_factors,
            "processing_time_ms": ensemble_result.processing_time_ms,
            "model_config": {"pipeline": "upload_revalidation"},
        }
        supabase.table("ai_results").insert(ai_record).execute()

        AuditService.log_system(
            event_type="ai_analysis_completed",
            resource_type="claim",
            resource_id=claim_id,
            event_data={
                "trigger": "document_upload_revalidation",
                "document_id": document_id,
                "recommendation": ensemble_result.recommendation,
                "confidence": ensemble_result.confidence,
                "auto_approval_eligible": auto_approval_eligible,
                "workflow_stage": workflow_stage,
            },
        )

        try:
            unique_suffix = document_id or datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            analyze_claim_async.apply_async(
                args=[claim_id],
                countdown=2,
                task_id=f"post-reval-{claim_id}-{unique_suffix}",
            )
        except Exception as exc:
            logger.warning(f"[Revalidation] Failed to queue async analysis for claim {claim_id}: {exc}")

        return {
            "status": "revalidated",
            "claim_id": claim_id,
            "document_id": document_id,
            "workflow_stage": workflow_stage,
            "claim_status": next_status,
            "auto_approval_eligible": auto_approval_eligible,
            "reviewer_suggestion": reviewer_suggestion,
            "extracted_medical_facts": facts.to_dict(),
            "matched_policies": facts.matched_policies,
            "detected_risks": facts.detected_risks,
            "violation_flags": violations,
            "recommendation": ensemble_result.recommendation,
            "confidence_score": ensemble_result.confidence,
            "trust_score": ensemble_result.trust_score,
        }

def get_revalidation_service() -> RevalidationService:
    return RevalidationService()
