"""
CareVerify - Celery Async Tasks
AI analysis pipeline, OCR, trust score recomputation, SLA monitoring
"""

from __future__ import annotations
import logging
from datetime import datetime

from app.extensions import celery_app

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# CLAIM AI ANALYSIS
# ─────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="app.tasks.ai_tasks.analyze_claim_async",
    max_retries=3,
    default_retry_delay=60,
    queue="ai_processing",
)
def analyze_claim_async(self, claim_id: str):
    """
    Full AI analysis pipeline for a submitted claim.
    
    Pipeline:
    1. Load claim + documents from DB
    2. Wait for OCR to complete (or trigger it)
    3. Feature engineering
    4. Ensemble AI scoring
    5. Store results + update claim status
    6. Trigger notifications
    """
    logger.info(f"[AI Pipeline] Starting analysis for claim {claim_id}")

    try:
        from app.services.supabase_client import get_supabase_admin
        from ai.pipeline.feature_engineering import FeatureEngineer
        from ai.models.ensemble_engine import get_ai_engine

        supabase = get_supabase_admin()

        # 1. Load claim
        claim_result = supabase.table("claims").select("*").eq("id", claim_id).single().execute()
        if not claim_result.data:
            logger.error(f"[AI Pipeline] Claim {claim_id} not found")
            return

        claim = claim_result.data

        # Update status
        supabase.table("claims").update({"status": "ai_analyzing"}).eq("id", claim_id).execute()

        # 2. Collect OCR data from all documents
        docs = supabase.table("claim_documents").select(
            "ocr_data, ocr_text, ocr_extracted"
        ).eq("claim_id", claim_id).execute().data

        combined_ocr_data = {}
        nlp_text = ""
        for doc in docs:
            if doc.get("ocr_extracted"):
                combined_ocr_data.update(doc.get("ocr_data") or {})
                nlp_text += (doc.get("ocr_text") or "") + "\n\n"

        # 3. Feature engineering
        engineer = FeatureEngineer(supabase)
        features = engineer.build_features(claim, combined_ocr_data)

        # 4. NLP on clinical text
        engine = get_ai_engine()
        nlp_score, nlp_entities = engine._run_nlp(nlp_text)
        features.nlp_inconsistency_score = nlp_score

        # 5. Ensemble scoring
        result = engine.analyze(features, nlp_text)

        # 6. Persist AI results
        ai_record = {
            "claim_id": claim_id,
            "model_version": result.model_version,
            "xgboost_fraud_score": result.xgboost_fraud_score,
            "rf_approval_score": result.rf_approval_score,
            "isolation_anomaly_score": result.isolation_anomaly_score,
            "autoencoder_anomaly_score": result.autoencoder_anomaly_score,
            "nlp_sentiment_score": result.nlp_sentiment_score,
            "nlp_entities": result.nlp_entities,
            "trust_score": result.trust_score,
            "final_recommendation": result.recommendation,
            "confidence": result.confidence,
            "feature_importances": result.feature_importances,
            "shap_values": result.shap_values,
            "explanation_text": result.explanation_text,
            "risk_factors": result.risk_factors,
            "processing_time_ms": result.processing_time_ms,
        }
        supabase.table("ai_results").insert(ai_record).execute()

        # 7. Update claim with AI scores
        next_status = {
            "AUTO_APPROVE": "pending_review",
            "APPROVE_WITH_REVIEW": "pending_review",
            "COMPLIANCE_REVIEW_REQUIRED": "compliance_review",
            "HIGH_RISK_HOLD": "compliance_review",
        }.get(result.recommendation, "pending_review")

        supabase.table("claims").update({
            "trust_score": result.trust_score,
            "fraud_probability": result.fraud_probability,
            "anomaly_score": result.anomaly_score,
            "approval_likelihood": result.approval_likelihood,
            "ai_recommendation": result.recommendation,
            "ai_explanation": {
                "explanation_text": result.explanation_text,
                "risk_factors": result.risk_factors,
            },
            "ai_analyzed_at": datetime.utcnow().isoformat(),
            "status": next_status,
        }).eq("id", claim_id).execute()

        # 8. Audit log
        from app.services.audit_service import AuditService
        AuditService.log_system(
            event_type="ai_analysis_completed",
            resource_type="claim",
            resource_id=claim_id,
            event_data={
                "trust_score": result.trust_score,
                "recommendation": result.recommendation,
                "processing_ms": result.processing_time_ms,
            }
        )

        # 9. Notify admin for high-risk claims
        if result.trust_score < 40:
            from app.services.notification_service import NotificationService
            NotificationService.notify_admin_high_risk(claim_id, result.trust_score)

        # 10. Trigger trust score recomputation for the hospital org
        recompute_trust_scores.apply_async(
            args=[claim["hospital_org_id"]],
            countdown=30,
        )

        logger.info(
            f"[AI Pipeline] Claim {claim_id} analyzed: "
            f"trust={result.trust_score}, rec={result.recommendation}"
        )

    except Exception as exc:
        logger.error(f"[AI Pipeline] Error analyzing claim {claim_id}: {exc}", exc_info=True)
        try:
            from app.services.supabase_client import get_supabase_admin
            get_supabase_admin().table("claims").update(
                {"status": "pending_review"}
            ).eq("id", claim_id).execute()
        except Exception:
            pass
        raise self.retry(exc=exc)


# ─────────────────────────────────────────
# OCR PROCESSING
# ─────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="app.tasks.ai_tasks.process_document_ocr",
    max_retries=3,
    default_retry_delay=30,
    queue="ai_processing",
)
def process_document_ocr(self, document_id: str, claim_id: str, storage_path: str):
    """
    Run OCR on an uploaded document.
    Downloads from Supabase Storage, extracts text, updates DB.
    """
    logger.info(f"[OCR] Processing document {document_id}")

    try:
        import os
        from app.services.supabase_client import get_supabase_admin
        from ai.pipeline.ocr_pipeline import get_ocr_pipeline

        supabase = get_supabase_admin()
        bucket = os.environ.get("SUPABASE_STORAGE_BUCKET", "medical-documents")

        # Get mime type
        doc = supabase.table("claim_documents").select(
            "mime_type, file_name"
        ).eq("id", document_id).single().execute().data

        if not doc:
            logger.error(f"[OCR] Document {document_id} not found")
            return

        # Download file
        file_bytes = supabase.storage.from_(bucket).download(storage_path)

        # Run OCR
        pipeline = get_ocr_pipeline()
        result = pipeline.process(file_bytes, doc["mime_type"])

        # Update document record
        supabase.table("claim_documents").update({
            "ocr_extracted": True,
            "ocr_text": result.raw_text[:100000] if result.raw_text else "",  # cap at 100k chars
            "ocr_confidence": result.confidence,
            "ocr_data": result.structured_data,
        }).eq("id", document_id).execute()

        logger.info(f"[OCR] Document {document_id} processed. Confidence: {result.confidence:.2%}")

        # Check if all documents for the claim are OCR'd
        all_docs = supabase.table("claim_documents").select(
            "ocr_extracted"
        ).eq("claim_id", claim_id).execute().data

        all_ocrd = all(d.get("ocr_extracted") for d in all_docs)

        if all_ocrd:
            # Check if claim is in submitted state (hasn't started AI yet)
            claim = supabase.table("claims").select("status").eq("id", claim_id).single().execute().data
            if claim and claim["status"] == "submitted":
                logger.info(f"[OCR] All documents OCR'd for claim {claim_id}. AI analysis will proceed.")

        revalidate_claim_after_upload.apply_async(
            args=[claim_id, document_id],
            countdown=1,
            task_id=f"reval-{claim_id}-{document_id}",
        )

    except Exception as exc:
        logger.error(f"[OCR] Error processing document {document_id}: {exc}", exc_info=True)
        raise self.retry(exc=exc)


@celery_app.task(
    bind=True,
    name="app.tasks.ai_tasks.revalidate_claim_after_upload",
    max_retries=2,
    default_retry_delay=20,
    queue="ai_processing",
)
def revalidate_claim_after_upload(self, claim_id: str, document_id: str):
    """
    Event-driven revalidation task:
    OCR completion -> extraction -> compliance/risk recompute.
    """
    logger.info(f"[RevalidationTask] Triggered for claim {claim_id}, document {document_id}")
    try:
        from app.services.revalidation_service import get_revalidation_service

        service = get_revalidation_service()
        result = service.revalidate_claim(claim_id=claim_id, document_id=document_id)
        logger.info(
            f"[RevalidationTask] Completed for claim {claim_id}: "
            f"status={result.get('status')} recommendation={result.get('recommendation')}"
        )
    except Exception as exc:
        logger.error(
            f"[RevalidationTask] Failed for claim {claim_id}, document {document_id}: {exc}",
            exc_info=True,
        )
        raise self.retry(exc=exc)


# ─────────────────────────────────────────
# TRUST SCORE RECOMPUTATION
# ─────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="app.tasks.ai_tasks.recompute_trust_scores",
    queue="default",
)
def recompute_trust_scores(self, org_id: str):
    """
    Recompute the Organization Trust Score based on recent claim history.
    
    Formula:
    - Claim approval rate (40%)
    - Fraud rate (30%, inverted)
    - SLA compliance (20%)
    - Documentation quality (10%)
    """
    logger.info(f"[TrustScore] Recomputing for org {org_id}")

    try:
        from app.services.supabase_client import get_supabase_admin
        from datetime import timedelta

        supabase = get_supabase_admin()

        cutoff = (datetime.utcnow() - timedelta(days=180)).isoformat()
        claims = supabase.table("claims").select(
            "status, fraud_probability, sla_breached, trust_score, approved_amount, claimed_amount"
        ).eq("hospital_org_id", org_id).gte("created_at", cutoff).execute().data

        if not claims:
            logger.info(f"[TrustScore] No claims for org {org_id}")
            return

        total = len(claims)
        approved = sum(1 for c in claims if c["status"] in ("approved", "partially_approved"))
        fraud_flags = sum(1 for c in claims if (c.get("fraud_probability") or 0) > 0.7)
        sla_breaches = sum(1 for c in claims if c.get("sla_breached"))
        avg_trust = sum(float(c.get("trust_score") or 50) for c in claims) / total

        approval_rate = approved / total
        fraud_rate = fraud_flags / total
        sla_compliance = 1.0 - (sla_breaches / total)
        doc_quality = min(1.0, avg_trust / 100)

        new_score = round(
            (approval_rate * 40 +
             (1.0 - fraud_rate) * 30 +
             sla_compliance * 20 +
             doc_quality * 10),
            2,
        )

        org = supabase.table("organizations").select("trust_score").eq("id", org_id).single().execute().data
        old_score = float(org["trust_score"]) if org else 50.0

        # Update organization
        supabase.table("organizations").update({"trust_score": new_score}).eq("id", org_id).execute()

        # Record historical entry
        supabase.table("organization_trust_scores").insert({
            "organization_id": org_id,
            "score": new_score,
            "previous_score": old_score,
            "factors": {
                "approval_rate": approval_rate,
                "fraud_rate": fraud_rate,
                "sla_compliance": sla_compliance,
                "doc_quality": doc_quality,
                "claim_sample_size": total,
            },
        }).execute()

        logger.info(f"[TrustScore] Org {org_id}: {old_score} → {new_score}")

    except Exception as exc:
        logger.error(f"[TrustScore] Error for org {org_id}: {exc}", exc_info=True)


# ─────────────────────────────────────────
# SLA MONITORING (Periodic)
# ─────────────────────────────────────────

@celery_app.task(name="app.tasks.ai_tasks.check_sla_breaches", queue="default")
def check_sla_breaches():
    """
    Periodic task: detect SLA breaches and notify admins.
    Schedule: every 30 minutes via Celery Beat.
    """
    logger.info("[SLA Monitor] Checking for SLA breaches")

    try:
        from app.services.supabase_client import get_supabase_admin
        from app.services.notification_service import NotificationService
        from app.services.audit_service import AuditService

        supabase = get_supabase_admin()
        now = datetime.utcnow().isoformat()

        breached = supabase.table("claims").select("id, claim_number, hospital_org_id").lt(
            "sla_deadline", now
        ).eq("sla_breached", False).in_(
            "status", ["submitted", "ocr_processing", "ai_analyzing", "pending_review", "compliance_review"]
        ).execute().data

        for claim in breached:
            supabase.table("claims").update({"sla_breached": True}).eq("id", claim["id"]).execute()
            NotificationService.notify_sla_breach(claim["id"])
            AuditService.log_system(
                event_type="sla_breach_detected",
                resource_type="claim",
                resource_id=claim["id"],
                event_data={"claim_number": claim["claim_number"]},
            )

        if breached:
            logger.warning(f"[SLA Monitor] {len(breached)} SLA breaches detected and marked")

    except Exception as e:
        logger.error(f"[SLA Monitor] Error: {e}", exc_info=True)


# ─────────────────────────────────────────
# QUEUE STATS HELPER
# ─────────────────────────────────────────

def get_queue_stats() -> dict:
    """Return Celery queue statistics for the admin dashboard."""
    try:
        from app.extensions import celery_app as app
        inspect = app.control.inspect()
        active = inspect.active() or {}
        reserved = inspect.reserved() or {}
        scheduled = inspect.scheduled() or {}

        return {
            "active_tasks": sum(len(v) for v in active.values()),
            "reserved_tasks": sum(len(v) for v in reserved.values()),
            "scheduled_tasks": sum(len(v) for v in scheduled.values()),
            "workers": list(active.keys()),
        }
    except Exception:
        return {"error": "Could not connect to Celery workers"}
