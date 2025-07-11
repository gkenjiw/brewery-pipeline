from airflow.utils.email import send_email
import logging
import requests

# To use email notification you should configure a smtp server in airflow.cfg
def notify_email(context):
    try:
        subject = f"[Airflow] DAG Failed: {context['dag'].dag_id}"
        body = f"""
        DAG: {context['dag'].dag_id}<br>
        Task: {context['task_instance'].task_id}<br>
        Execution Time: {context['execution_date']}<br>
        Log: <a href="{context['task_instance'].log_url}">Log Link</a><br>
        """
        send_email(to=["seu@email.com"], subject=subject, html_content=body)
    except Exception as e:
        logging.error(f"Notifier failed: {e}")

# It's possible to send notifications to slack
def notify_slack(context):
    import requests

    webhook_url = "https://hooks.slack.com/services/SEU/WEBHOOK/AQUI"
    message = f""":red_circle: DAG *{context['dag'].dag_id}* failed.
    *Task:* {context['task_instance'].task_id}
    *Execution Time:* {context['execution_date']}
    *Log URL:* {context['task_instance'].log_url}
    """
    try:
        requests.post(webhook_url, json={"text": message})
    except Exception as e:
        logging.error(f"Failed to send Slack notification: {e}")

# Since I don't have a Slack license, I'm using a Discord webhook to send notifications

def notify_discord_failure(context):
    webhook_url = "https://discord.com/api/webhooks/1393218030095568966/Qg5GHG0wCY0sUp1gYQm4ckifUsEr9T11vwN9aajoTqXLCYjbCQP96Mo2DFlVXJ1VBhP9"
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    # log_url = context.get("task_instance").log_url # log_url is not available, should be in next version https://github.com/apache/airflow/pull/50376

    message = {
        "content": f":rotating_light: **Pipeline failed!**\n"
                    f"DAG: **{dag_id}**\n"
                    f"Task: `{task_id}`\n"
                    f"Execution Time: `{execution_date}`\n"
                #    f"[View Log]({log_url})"
    }

    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        logging.info("Discord notification sent.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Discord notification: {e}")

def notify_discord_success(context):
    webhook_url = "https://discord.com/api/webhooks/1393218030095568966/Qg5GHG0wCY0sUp1gYQm4ckifUsEr9T11vwN9aajoTqXLCYjbCQP96Mo2DFlVXJ1VBhP9"
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    # log_url = context.get("task_instance").log_url # log_url is not available, should be in next version https://github.com/apache/airflow/pull/50376

    message = {
        "content": f":white_check_mark: **Success!**\n"
                    f"DAG: **{dag_id}**\n"
                    f"Task: `{task_id}`\n"
                    f"Execution Time: `{execution_date}`\n"
                   # f"[View Log]({log_url})"
    }

    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        logging.info("Discord notification sent.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Discord notification: {e}")