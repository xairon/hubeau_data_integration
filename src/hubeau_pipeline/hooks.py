"""
Dagster Failure Hooks - Notification & Alerting

Hooks for failure notification when scheduled jobs fail.
Configure with environment variables for alerting destinations.
"""

from dagster import (
    failure_hook,
    success_hook,
    HookContext,
)
import os
import logging

logger = logging.getLogger(__name__)


# ==============================================================================
# FAILURE HOOKS
# ==============================================================================

@failure_hook
def log_failure_hook(context: HookContext):
    """
    Log detailed failure information.
    Always active for debugging purposes.
    """
    op_name = context.op.name if context.op else "unknown"
    job_name = context.job_name if hasattr(context, "job_name") else "unknown"
    run_id = context.run_id if hasattr(context, "run_id") else "unknown"
    
    # Get exception details if available
    op_exception = context.op_exception if hasattr(context, "op_exception") else None
    exception_str = str(op_exception) if op_exception else "No exception details"
    
    logger.error(
        f"❌ PIPELINE FAILURE\n"
        f"  Job: {job_name}\n"
        f"  Op: {op_name}\n"
        f"  Run ID: {run_id}\n"
        f"  Error: {exception_str}"
    )


@failure_hook
def slack_failure_hook(context: HookContext):
    """
    Send Slack notification on failure.
    Requires SLACK_WEBHOOK_URL environment variable.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return  # Slack not configured
    
    import requests
    
    op_name = context.op.name if context.op else "unknown"
    job_name = context.job_name if hasattr(context, "job_name") else "unknown"
    run_id = context.run_id if hasattr(context, "run_id") else "unknown"
    
    op_exception = context.op_exception if hasattr(context, "op_exception") else None
    exception_str = str(op_exception)[:500] if op_exception else "No details"
    
    dagster_url = os.environ.get("DAGSTER_WEBSERVER_URL", "http://localhost:3000")
    run_url = f"{dagster_url}/runs/{run_id}"
    
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ Pipeline Failure Alert",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job:*\n{job_name}"},
                    {"type": "mrkdwn", "text": f"*Operation:*\n{op_name}"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{exception_str}```"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Run"},
                        "url": run_url
                    }
                ]
            }
        ]
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"✅ Slack notification sent for failed run {run_id}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to send Slack notification: {e}")


@failure_hook
def email_failure_hook(context: HookContext):
    """
    Send email notification on failure.
    Requires SMTP_* environment variables.
    """
    smtp_host = os.environ.get("SMTP_HOST")
    if not smtp_host:
        return  # Email not configured
    
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_password = os.environ.get("SMTP_PASSWORD", "")
    # Default alert email set for user
    alert_email = os.environ.get("ALERT_EMAIL", "nicolas.ringuet@univ-tours.fr")
    
    if not alert_email:
        return
    
    op_name = context.op.name if context.op else "unknown"
    job_name = context.job_name if hasattr(context, "job_name") else "unknown"
    run_id = context.run_id if hasattr(context, "run_id") else "unknown"
    
    op_exception = context.op_exception if hasattr(context, "op_exception") else None
    exception_str = str(op_exception) if op_exception else "No details"
    
    msg = MIMEMultipart()
    msg["Subject"] = f"[DAGSTER ALERT] Pipeline Failure: {job_name}"
    msg["From"] = smtp_user or "dagster@localhost"
    msg["To"] = alert_email
    
    body = f"""
    ❌ Pipeline Failure Alert
    
    Job: {job_name}
    Operation: {op_name}
    Run ID: {run_id}
    
    Error:
    {exception_str}
    """
    
    msg.attach(MIMEText(body, "plain"))
    
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if smtp_user and smtp_password:
                server.starttls()
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
        logger.info(f"✅ Email notification sent for failed run {run_id}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to send email notification: {e}")


# ==============================================================================
# SUCCESS HOOKS (Optional)
# ==============================================================================

@success_hook
def log_success_hook(context: HookContext):
    """
    Log successful completion of operations.
    """
    op_name = context.op.name if context.op else "unknown"
    logger.info(f"✅ Operation completed successfully: {op_name}")


# ==============================================================================
# EXPORTS
# ==============================================================================

all_failure_hooks = [
    log_failure_hook,
    slack_failure_hook,
    email_failure_hook,
]

all_success_hooks = [
    log_success_hook,
]
