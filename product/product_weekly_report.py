# Сводный отчет за неделю по клиентам
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.table import Table, TableStyleInfo
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib
import pytz 

# Настройки SMTP для отправки электронной почты
def send_email(subject, to_emails, attachment_path):
    smtp_server = 'smtp.mail.ru'
    smtp_port = 465
    smtp_user = 'xxxxxx@e.ru'
    smtp_password = '*************'

    # Создание объекта MIMEMultipart для формирования письма
    msg = MIMEMultipart()
    msg['From'] = 'xxxxxx@e.ru'
    msg['To'] = ', '.join(to_emails)
    msg['Subject'] = subject

    # Добавление вложения к письму
    with open(attachment_path, 'rb') as attachment:
        excel_part = MIMEApplication(
            attachment.read(),
            Name="report.xlsx"
        )
    excel_part['Content-Disposition'] = f'attachment; filename="report.xlsx"'
    msg.attach(excel_part)

    # Отправка письма по почте
    with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(smtp_user, smtp_password)
        server.sendmail(msg['From'], to_emails, msg.as_string())

# Настройки соединения с PostgreSQL
def generate_report(**kwargs):
    conn_id = 'p_prod'
    
    sql_queries = [
        """
        select a."Id" as "Account id",
        a."ExternalAccountId",
        a."Phone",
        o."Id"as "Order id",
        o."UniqId" as "Uniq order id",
        o."CreatedAt",
        o."UpdatedAt",
        cast(o."ExpirationDate" AT TIME ZONE 'UTC' AS timestamp),
        cast(o."CancellationDate" AT TIME ZONE 'UTC' AS timestamp),
        o."Status",
        o."CancellationByBuyerId",
        o."CancellationByPharmacyId",
        o."CancellationBySystemId",
        oc."Code",
        oc."RootCause",
        o."PharmacyName",
        ppl."FullAddress",
        pp."ExternalId" as "External pharmacy id",
        pp."SystemPublicId" as "System Public pharmacy Id",
        pp."ExternalId" as "External pharmacy Id",
        pp."ExternalPublicId" as "External public pharmacy Id",
        pp."ExternalPartnerId",
        pp."ExternalPharmacyNetworkId"
        from orders o
        join accounts a on a."Id" = o."AccountId"
        join partners_pharmacies pp on o."PharmacyId" = pp."Id"
        join partners_pharmacies_location ppl on pp."Id" = ppl."PharmacyId"
        left join orders_cancellation oc on oc."Id" = o."CancellationByPharmacyId" 
        or oc."Id" = o."CancellationByBuyerId" or oc."Id" = o."CancellationBySystemId"
        WHERE o."CreatedAt" >= now() AT TIME ZONE 'UTC' - INTERVAL '14d'
        """
    ]

    # Создаем файл Excel
    wb = Workbook()

    # Выполняем каждый SQL-запрос и добавляем результат в отдельный лист
    for i, (sql_query, sheet_name) in enumerate(zip(sql_queries, ["Данные по заказам"])):
        # Подключаемся к PostgreSQL и получаем результат SQL-запроса
        hook = PostgresHook(postgres_conn_id=conn_id)
        rows = hook.get_pandas_df(sql_query)
        df = pd.DataFrame(rows)

        # Создаем лист с указанным именем
        ws = wb.create_sheet(title=sheet_name)

        # Записываем результат в лист
        for row in dataframe_to_rows(df, index=False, header=True):
            ws.append(row)

        # Создаем таблицу Excel для данных
        table = Table(displayName=f'Table_{i + 1}', ref=f'A1:{chr(65 + len(df.columns) - 1)}{len(df) + 1}')
        style = TableStyleInfo(
            name='TableStyleMedium9', showFirstColumn=False,
            showLastColumn=False, showRowStripes=True, showColumnStripes=True)
        table.tableStyleInfo = style
        ws.add_table(table)

    # Используем значение по умолчанию для output_dir, если 'output_dir' отсутствует в dag_run.conf
    output_path = os.path.join(kwargs['dag_run'].conf.get('output_dir', '/tmp'), 'report.xlsx')

    # Сохраняем файл Excel
    wb.remove(wb.active)  # Удаляем первый лист
    wb.save(output_path)

    # Отправляем отчет по электронной почте
    email_recipients = ['xxxxxx@m.ru']
    send_email('Отчет', email_recipients, output_path)
    return output_path

# Настройки DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'product_report',
    default_args=default_args,
    description='cancel orders',
    schedule_interval='0 4 * * 1',  # Установите интервал выполнения по своему усмотрению
)


# Задача для генерации отчета
generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)
