from airflow.utils.email import send_email_smtp # direct SMTP call

def send_pretty_email_failure(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag_id') or task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    hostname = context.get('hostname', 'unknown')

    subject = f"🚨 Airflow Task Failed: {task_id} in DAG {dag_id}"

    html_content = f"""
    <html>
    <body style="font-family:Arial, sans-serif; background:#f8f9fa; color:#333;">
        <div style="max-width:600px; margin:auto; background:white; border-radius:10px; padding:20px; box-shadow:0 0 10px rgba(0,0,0,0.1);">
            <h2 style="color:#d9534f;">🚨 Task Failed</h2>
            <p><strong>DAG:</strong> {dag_id}</p>
            <p><strong>Task:</strong> {task_id}</p>
            <p><strong>Execution Date:</strong> {execution_date}</p>
            <p><strong>Try Number:</strong> {task_instance.try_number}</p>

            <p style="margin-top:20px;">
                <a href="{log_url}" style="background:#0275d8; color:white; padding:10px 15px; text-decoration:none; border-radius:5px;">
                    View Logs
                </a>
            </p>

            <hr>
            <p style="font-size:12px; color:#777;">Airflow Host: {hostname}</p>
        </div>
    </body>
    </html>
    """

    send_email_smtp(
        to=["mmzidane101@gmail.com"],
        subject=subject,
        html_content=html_content
    )


def send_pretty_email_success(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag_id') or task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    hostname = context.get('hostname', 'unknown')

    subject = f"✅ Airflow Task Succeeded: {task_id} in DAG {dag_id}"

    html_content = f"""
    <html>
    <body style="font-family:Arial, sans-serif; background:#f8f9fa; color:#333;">
        <div style="max-width:600px; margin:auto; background:white; border-radius:10px; padding:20px; box-shadow:0 0 10px rgba(0,0,0,0.1);">
            <h2 style="color:#28a745;">✅ Task Succeeded</h2>
            <p><strong>DAG:</strong> {dag_id}</p>
            <p><strong>Task:</strong> {task_id}</p>
            <p><strong>Execution Date:</strong> {execution_date}</p>

            <p style="margin-top:20px;">
                <a href="{log_url}" style="background:#28a745; color:white; padding:10px 15px; text-decoration:none; border-radius:5px;">
                    View Logs
                </a>
            </p>

            <hr>
            <p style="font-size:12px; color:#777;">Airflow Host: {hostname}</p>
        </div>
    </body>
    </html>
    """

    send_email_smtp(
        to=["mmzidane101@gmail.com"],
        subject=subject,
        html_content=html_content
    )
