import os
import sqlite3
import queue
import threading
import time
import uuid
import requests
import paramiko
import hmac
import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, send_file
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import flask
from dotenv import load_dotenv
import sqlite3
from datetime import datetime
import jwt
import time

sqlite3.register_adapter(datetime, lambda val: val.isoformat())
load_dotenv('/opt/pr-tester/.env')

app = Flask(__name__)

GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GITHUB_WEBHOOK_SECRET = os.environ.get('GITHUB_WEBHOOK_SECRET')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

DB_DIR = '/opt/pr-tester'
DB_PATH = os.path.join(DB_DIR, 'pr_tests.db')

FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
FONT_SIZE_TITLE = 36
FONT_SIZE_TEXT = 24
GOLD_COLOR = (255, 215, 0)
AVATAR_SIZE = 40
GITHUB_AVATAR_URL = "https://github.com/"

YC_OAUTH_TOKEN = os.environ.get('YC_OAUTH_TOKEN')
YC_FOLDER_ID = os.environ.get('YC_FOLDER_ID')
WORKER_VM_ID = os.environ.get('WORKER_VM_ID')
YC_API_URL = "https://compute.api.cloud.yandex.net/compute/v1/"

WORKER_USER = os.environ.get('WORKER_USER', 'ubuntu')
WORKER_PASSWORD = os.environ.get('WORKER_PASSWORD')
GPU_WORKER_IP = os.environ.get('GPU_WORKER_IP')

YC_SERVICE_ACCOUNT_KEY_PATH = os.environ.get('YC_SERVICE_ACCOUNT_KEY_PATH', '/opt/pr-tester/key.json')
YC_SERVICE_ACCOUNT_KEY = None

# Конфигурация таймаутов для перезапуска задач
TASK_PROCESSING_TIMEOUT = 1200  # 20 минут в секундах (нормальное время 10 минут + запас)
TASK_PENDING_TIMEOUT = 1800     # 30 минут в секундах
STUCK_TASK_CHECK_INTERVAL = 300  # 5 минут
MAX_RESTART_ATTEMPTS = 3  # Максимальное количество перезапусков

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

task_queue = queue.Queue()
worker_lock = threading.Lock()
worker_active = False
worker_last_heartbeat = time.time()
worker_current_ip = None

USE_YC_CLOUD = os.environ.get('USE_YC_CLOUD', 'false').lower() == 'true'


def load_service_account_key():
    global YC_SERVICE_ACCOUNT_KEY
    try:
        with open(YC_SERVICE_ACCOUNT_KEY_PATH, 'r') as f:
            YC_SERVICE_ACCOUNT_KEY = json.load(f)
        logger.info("✅ Сервисный ключ Яндекс.Облака успешно загружен")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки сервисного ключа: {e}")
        return False


def get_iam_token():
    if not YC_SERVICE_ACCOUNT_KEY:
        if not load_service_account_key():
            return None

    try:
        service_account_id = YC_SERVICE_ACCOUNT_KEY["service_account_id"]
        key_id = YC_SERVICE_ACCOUNT_KEY["id"]
        private_key = YC_SERVICE_ACCOUNT_KEY["private_key"]

        now = int(time.time())
        payload = {
            "aud": "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            "iss": service_account_id,
            "iat": now,
            "exp": now + 3600,
        }

        encoded_jwt = jwt.encode(payload, private_key, algorithm="PS256", headers={"kid": key_id})

        response = requests.post(
            "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            json={"jwt": encoded_jwt},
            timeout=15,
        )

        if response.status_code == 200:
            token = response.json()["iamToken"]
            logger.info("✅ IAM токен успешно создан через JWT")
            return token
        else:
            logger.error(f"❌ Ошибка получения IAM токена: {response.status_code} {response.text}")
    except Exception as e:
        logger.error(f"❌ Ошибка при генерации IAM токена: {e}")
    return None


def ensure_db_dir():
    if not os.path.exists(DB_DIR):
        logger.error(f"Database directory does not exist: {DB_DIR}")
        raise Exception(f"Database directory does not exist: {DB_DIR}")


def init_db():
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS tests
                 (id TEXT PRIMARY KEY,
                  pr_id INTEGER,
                  repo TEXT,
                  status TEXT,
                  result_json TEXT,
                  logs TEXT,
                  created_at TIMESTAMP,
                  updated_at TIMESTAMP,
                  commit_sha TEXT,
                  comment_id INTEGER)''')

    c.execute('''CREATE TABLE IF NOT EXISTS tasks
                 (id TEXT PRIMARY KEY,
                  pr_id INTEGER,
                  repo TEXT,
                  clone_url TEXT,
                  commit_sha TEXT,
                  branch TEXT,
                  status TEXT CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
                  created_at TIMESTAMP,
                  started_at TIMESTAMP,
                  completed_at TIMESTAMP,
                  comment_id INTEGER,
                  restart_count INTEGER DEFAULT 0,
                  max_restarts INTEGER DEFAULT 3)''')

    c.execute('''CREATE TABLE IF NOT EXISTS worker_status
                  (id INTEGER PRIMARY KEY AUTOINCREMENT,
                   status TEXT,
                   last_heartbeat TIMESTAMP,
                   current_task TEXT,
                   created_at TIMESTAMP)''')

    c.execute('''CREATE TABLE IF NOT EXISTS processed_events
                 (event_id TEXT PRIMARY KEY,
                  event_type TEXT,
                  repo TEXT,
                  pr_id INTEGER,
                  created_at TIMESTAMP)''')

    conn.commit()
    conn.close()


def migrate_db():
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        c.execute("PRAGMA table_info(tasks)")
        columns = [column[1] for column in c.fetchall()]

        if 'comment_id' not in columns:
            c.execute("ALTER TABLE tasks ADD COLUMN comment_id INTEGER")
            logger.info("Added comment_id column to tasks table")

        if 'restart_count' not in columns:
            c.execute("ALTER TABLE tasks ADD COLUMN restart_count INTEGER DEFAULT 0")
            logger.info("Added restart_count column to tasks table")

        if 'max_restarts' not in columns:
            c.execute("ALTER TABLE tasks ADD COLUMN max_restarts INTEGER DEFAULT 3")
            logger.info("Added max_restarts column to tasks table")

        c.execute("PRAGMA table_info(tests)")
        columns = [column[1] for column in c.fetchall()]

        if 'comment_id' not in columns:
            c.execute("ALTER TABLE tests ADD COLUMN comment_id INTEGER")
            logger.info("Added comment_id column to tests table")

        conn.commit()

    except Exception as e:
        logger.error(f"Database migration failed: {e}")
    finally:
        conn.close()


def get_worker_ip():
    global worker_current_ip

    if not USE_YC_CLOUD:
        return worker_current_ip

    token = get_iam_token()
    if not token:
        logger.error("Не удалось получить IAM токен")
        return None

    headers = {
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.get(f"{YC_API_URL}instances/{WORKER_VM_ID}",
                                headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            network_interfaces = data.get('networkInterfaces', [])
            if network_interfaces:
                primary_interface = network_interfaces[0]
                ip_address = primary_interface.get('primaryV4Address', {}).get('address')
                worker_current_ip = ip_address
                return ip_address
        else:
            logger.error(f"❌ Ошибка получения информации о ВМ: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка получения IP ВМ: {e}")
        return None


def cleanup_old_tasks(pr_id, repo, current_task_id=None):
    """Удаляет старые задачи для PR при создании нового перезапуска"""
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Удаляем все pending и processing задачи для этого PR, кроме текущей (если указана)
        if current_task_id:
            c.execute("""
                DELETE FROM tasks 
                WHERE pr_id = ? AND repo = ? AND status IN ('pending', 'processing') AND id != ?
            """, (pr_id, repo, current_task_id))
        else:
            c.execute("""
                DELETE FROM tasks 
                WHERE pr_id = ? AND repo = ? AND status IN ('pending', 'processing')
            """, (pr_id, repo))

        deleted_count = c.rowcount
        conn.commit()
        conn.close()

        if deleted_count > 0:
            logger.info(f"🧹 Удалено {deleted_count} старых задач для PR #{pr_id} в {repo}")

        return deleted_count

    except Exception as e:
        logger.error(f"❌ Ошибка при удалении старых задач для PR #{pr_id}: {e}")
        return 0


def can_restart_task(task_id, pr_id, repo):
    """Проверяет, можно ли перезапустить задачу"""
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            SELECT restart_count, max_restarts, status 
            FROM tasks 
            WHERE id = ? AND pr_id = ? AND repo = ?
        """, (task_id, pr_id, repo))

        task = c.fetchone()
        conn.close()

        if not task:
            logger.warning(f"Задача {task_id} не найдена")
            return False

        restart_count, max_restarts, status = task

        if restart_count >= max_restarts:
            logger.info(f"❌ Достигнут лимит перезапусков для задачи {task_id} ({restart_count}/{max_restarts})")
            return False

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке возможности перезапуска: {e}")
        return False


def check_and_restart_stuck_tasks():
    """Проверяет и перезапускает зависшие задачи с учетом ограничений"""
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        current_time = datetime.now(timezone.utc)

        # Находим задачи в статусе processing, которые висят слишком долго (больше 20 минут)
        processing_timeout = current_time - timedelta(seconds=TASK_PROCESSING_TIMEOUT)
        c.execute("""
            SELECT id, pr_id, repo, clone_url, commit_sha, branch, comment_id, restart_count, max_restarts
            FROM tasks
            WHERE status = 'processing' AND started_at < ?
        """, (processing_timeout,))

        stuck_processing_tasks = c.fetchall()

        # Находим задачи в статусе pending, которые висят слишком долго (больше 30 минут)
        pending_timeout = current_time - timedelta(seconds=TASK_PENDING_TIMEOUT)
        c.execute("""
            SELECT id, pr_id, repo, clone_url, commit_sha, branch, comment_id, restart_count, max_restarts
            FROM tasks
            WHERE status = 'pending' AND created_at < ?
        """, (pending_timeout,))

        stuck_pending_tasks = c.fetchall()

        all_stuck_tasks = stuck_processing_tasks + stuck_pending_tasks
        restarted_count = 0

        for task in all_stuck_tasks:
            task_id, pr_id, repo, clone_url, commit_sha, branch, comment_id, restart_count, max_restarts = task

            # Проверяем возможность перезапуска
            if restart_count >= max_restarts:
                logger.warning(f"❌ Достигнут лимит перезапусков для зависшей задачи {task_id}, пропускаем")
                continue

            logger.warning(f"🔄 Найдена зависшая задача {task_id} для PR #{pr_id} в {repo}, перезапускаем... (попытка {restart_count + 1}/{max_restarts})")

            # Удаляем старые задачи перед перезапуском
            cleanup_old_tasks(pr_id, repo, task_id)

            # Обновляем статус задачи, сбрасываем время начала и увеличиваем счетчик перезапусков
            c.execute("""
                UPDATE tasks
                SET status = 'pending', started_at = NULL, completed_at = NULL, restart_count = restart_count + 1
                WHERE id = ?
            """, (task_id,))

            # Возвращаем задачу в очередь
            task_data = {
                'id': task_id,
                'pr_id': pr_id,
                'repo': repo,
                'clone_url': clone_url,
                'commit_sha': commit_sha,
                'branch': branch,
                'comment_id': comment_id
            }
            task_queue.put(task_data)
            restarted_count += 1

            # Логируем событие
            send_telegram_message(f"🔄 Перезапуск зависшей задачи для PR #{pr_id} в {repo} (попытка {restart_count + 1}/{max_restarts})")

        conn.commit()
        conn.close()

        if restarted_count > 0:
            logger.info(f"✅ Перезапущено {restarted_count} зависших задач")

    except Exception as e:
        logger.error(f"❌ Ошибка при проверке зависших задач: {e}")


def stuck_task_monitor():
    """Монитор для периодической проверки зависших задач"""
    while True:
        try:
            check_and_restart_stuck_tasks()
            time.sleep(STUCK_TASK_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"❌ Ошибка в мониторе зависших задач: {e}")
            time.sleep(60)  # Пауза при ошибке


def worker_monitor():
    global worker_active, worker_last_heartbeat, worker_current_ip

    while True:
        try:
            if USE_YC_CLOUD:
                current_ip = get_worker_ip()
                if current_ip:
                    logger.info(f"Worker VM IP: {current_ip}")

            with worker_lock:
                is_active = worker_active
                last_hb = worker_last_heartbeat

            if is_active and time.time() - last_hb > 300:
                logger.warning("Worker appears to be dead. Marking as inactive.")
                with worker_lock:
                    worker_active = False

            if not task_queue.empty() and not is_active:
                logger.info("Tasks in queue but worker is inactive. Starting worker...")
                if USE_YC_CLOUD:
                    if start_worker_vm():
                        with worker_lock:
                            worker_active = True
                            worker_last_heartbeat = time.time()
                else:
                    with worker_lock:
                        worker_active = True
                        worker_last_heartbeat = time.time()
                    logger.info("Worker marked as active (YC Cloud disabled)")

            if task_queue.empty() and is_active and time.time() - last_hb > 600 and USE_YC_CLOUD:
                logger.info("No tasks and worker idle. Stopping worker...")
                if stop_worker_vm():
                    with worker_lock:
                        worker_active = False

            time.sleep(30)
        except Exception as e:
            logger.error(f"Error in worker monitor: {e}")
            time.sleep(60)


def start_worker_vm():
    if not USE_YC_CLOUD:
        logger.info("Yandex Cloud disabled, skipping VM start")
        return True

    token = get_iam_token()
    if not token:
        logger.error("❌ Не удалось получить IAM токен для запуска ВМ")
        return False

    headers = {
        "Authorization": f"Bearer {token}",
    }

    try:
        logger.info(f"🟡 Отправка команды на запуск ВМ {WORKER_VM_ID}")
        response = requests.post(f"{YC_API_URL}instances/{WORKER_VM_ID}:start",
                                 headers=headers, timeout=30)

        if response.status_code == 200:
            logger.info("✅ Команда на запуск ВМ успешно отправлена")

            logger.info("⏳ Ожидание запуска ВМ...")
            for i in range(30):
                time.sleep(10)
                ip_address = get_worker_ip()
                if ip_address:
                    logger.info(f"✅ ВМ запущена с IP: {ip_address}")

                    logger.info("⏳ Ожидание инициализации ВМ...")
                    time.sleep(30)

                    return True
                logger.info(f"⏳ Попытка {i+1}/30: ВМ еще не готова...")

            logger.error("❌ Таймаут запуска ВМ")
            return False
        else:
            logger.error(f"❌ Ошибка запуска ВМ: {response.status_code} {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске ВМ: {e}")
        return False


def generate_comment(result, pr_id, repo, status, logs):
    if status == 'completed':
        emoji = '✅'
    elif status == 'failure':
        emoji = '❌'
    elif status == 'timeout':
        emoji = '⏰'
    else:
        emoji = '⚠️'

    comment = f"""{emoji} **Результаты тестирования PR #{pr_id}**


<details><summary>Логи тестирования (нажмите чтобы развернуть)</summary>
<pre>
{logs}
</pre>
</details>

[Посмотреть полные логи](http://{os.environ.get('MAIN_SERVER_IP', 'localhost')}:5000/logs/{pr_id})"""
    return comment


def post_github_comment(repo, pr_id, comment):
    if not GITHUB_TOKEN:
        logger.warning("No GITHUB_TOKEN, skipping comment")
        return
    url = f'https://api.github.com/repos/{repo}/issues/{pr_id}/comments'
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    data = {'body': comment}

    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 201:
            logger.info(f"Comment successfully posted to PR #{pr_id} in {repo}")
            return response.json().get('id')
        else:
            logger.error(f"Failed to post comment to PR #{pr_id} in {repo}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error posting GitHub comment: {e}")
        return None


def stop_worker_vm():
    if not USE_YC_CLOUD:
        logger.info("Yandex Cloud disabled, skipping VM stop")
        return True

    token = get_iam_token()
    if not token:
        logger.error("❌ Не удалось получить IAM токен для остановки ВМ")
        return False

    headers = {
        "Authorization": f"Bearer {token}",
    }

    try:
        logger.info(f"🟡 Отправка команды на остановку ВМ {WORKER_VM_ID}")
        response = requests.post(f"{YC_API_URL}instances/{WORKER_VM_ID}:stop",
                                 headers=headers, timeout=30)

        if response.status_code == 200:
            logger.info("✅ Команда на остановку ВМ успешно отправлена")

            logger.info("⏳ Ожидание остановки ВМ...")
            for i in range(30):
                time.sleep(10)
                status = get_vm_status()
                if status == "STOPPED":
                    logger.info("✅ ВМ успешно остановлена")
                    return True
                elif status == "STOPPING":
                    logger.info(f"⏳ Попытка {i+1}/30: ВМ останавливается...")
                    continue
                else:
                    logger.error(f"❌ Неожиданный статус ВМ при остановке: {status}")
                    return False

            logger.error("❌ Таймаут остановки ВМ")
            return False
        else:
            logger.error(f"❌ Ошибка остановки ВМ: {response.status_code} {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка при остановке ВМ: {e}")
        return False


def get_vm_status(vm_id=None):
    if not USE_YC_CLOUD:
        return "RUNNING"

    if vm_id is None:
        vm_id = WORKER_VM_ID

    token = get_iam_token()
    if not token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.get(f"{YC_API_URL}instances/{vm_id}",
                                headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json().get('status')
        else:
            logger.error(f"❌ Ошибка получения статуса ВМ: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка получения статуса ВМ: {e}")
        return None


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=data)
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
        return None


def check_ssh_connection(ip_address):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip_address, username=WORKER_USER, password=WORKER_PASSWORD, timeout=10)
        ssh.close()
        return True, "SSH connection successful"
    except paramiko.AuthenticationException:
        return False, "SSH authentication failed"
    except Exception as e:
        return False, f"SSH connection error: {str(e)}"


def generate_telegram_message(result, pr_id, repo, status, logs):
    if status == 'completed':
        emoji = '✅'
        status_text = 'Тесты пройдены успешно!'
    elif status == 'failure':
        emoji = '❌'
        status_text = 'Тесты не пройдены.'
    elif status == 'timeout':
        emoji = '⏰'
        status_text = 'Превышено время выполнения.'
    else:
        emoji = '⚠️'
        status_text = 'Произошла ошибка.'

    full_logs = logs

    if len(full_logs) > 1000:
        full_logs = full_logs[:1000] + "\n... (логи обрезаны)"

    bandwidth_info = ""
    if result.get('timings'):
        for key, value in result['timings'].items():
            if value and value > 0 and 'bandwidth' in key.lower():
                bandwidth_info = f"\n🏎️ Пропускная способность: {value:.2f} GB/s"
                break

    message = f"""
        {emoji} **Результаты тестирования PR #{pr_id}**
        📊 **Репозиторий:** {repo}
        🔄 **Статус:** {status_text}
        {bandwidth_info}

        **Логи тестирования:**
        📋 Полные логи доступны по ссылке:
        http://{os.environ.get('MAIN_SERVER_IP', 'localhost')}:5000/logs/{pr_id}
            """

    return message.strip()


def create_task_from_pr(pr_data, repo, event_type="pull_request", comment_id=None):
    global worker_active, worker_last_heartbeat
    pr_id = pr_data['number']
    clone_url = pr_data['head']['repo']['clone_url']
    commit_sha = pr_data['head']['sha']
    branch = pr_data['head']['ref']

    logger.info(f"Processing {event_type} for PR #{pr_id} from {repo}, branch: {branch}")

    # Очищаем старые задачи для этого PR перед созданием новой
    cleanup_old_tasks(pr_id, repo)

    # Создаем новую задачу
    task_id = str(uuid.uuid4())

    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        "INSERT INTO tasks (id, pr_id, repo, clone_url, commit_sha, branch, status, created_at, comment_id, restart_count, max_restarts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (task_id, pr_id, repo, clone_url, commit_sha, branch, 'pending', datetime.now(timezone.utc), comment_id, 0, MAX_RESTART_ATTEMPTS))
    conn.commit()
    conn.close()

    task = {
        'id': task_id,
        'pr_id': pr_id,
        'repo': repo,
        'clone_url': clone_url,
        'commit_sha': commit_sha,
        'branch': branch,
        'comment_id': comment_id
    }
    task_queue.put(task)

    with worker_lock:
        if not worker_active:
            logger.info("Starting worker for new task")
            if start_worker_vm():
                worker_active = True
                worker_last_heartbeat = time.time()

    logger.info(f"✅ Добавлена задача {task_id} для PR #{pr_id} в очередь")
    send_telegram_message(f"🚀 Новый PR #{pr_id} в {repo} добавлен в очередь")


def check_missed_prs():
    if not GITHUB_TOKEN:
        logger.warning("No GITHUB_TOKEN, cannot check missed PRs")
        return

    logger.info("Checking for missed PRs and comments...")

    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT repo FROM tasks")
    repos = [row[0] for row in c.fetchall()]
    conn.close()

    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }

    for repo in repos:
        try:
            prs_url = f'https://api.github.com/repos/{repo}/pulls?state=open'
            response = requests.get(prs_url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.error(f"Failed to get PRs for {repo}: {response.status_code}")
                continue

            pr_list = response.json()

            for pr in pr_list:
                pr_id = pr['number']

                ensure_db_dir()
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT id, status, restart_count, max_restarts FROM tasks WHERE pr_id = ? AND repo = ? ORDER BY created_at DESC LIMIT 1",
                          (pr_id, repo))
                task = c.fetchone()
                conn.close()

                # Создаем новую задачу только если предыдущая завершена и не превышен лимит перезапусков
                if not task or (task[1] in ('failed', 'timeout') and task[2] < task[3]):
                    logger.info(f"Found missed PR #{pr_id} in {repo}, creating task")
                    create_task_from_pr(pr, repo, "missed_pr_check")

                comments_url = f'https://api.github.com/repos/{repo}/issues/{pr_id}/comments'
                comments_response = requests.get(comments_url, headers=headers, timeout=30)

                if comments_response.status_code == 200:
                    comments = comments_response.json()

                    for comment in comments:
                        comment_id = comment['id']
                        comment_body = comment['body']
                        created_at = comment['created_at']

                        if '/run-tests' in comment_body:
                            ensure_db_dir()
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute("SELECT id FROM processed_events WHERE event_id = ?", (f"comment_{comment_id}",))
                            processed = c.fetchone()
                            conn.close()

                            if not processed:
                                logger.info(f"Found test command in comment {comment_id} for PR #{pr_id} in {repo}")
                                create_task_from_pr(pr, repo, "comment", comment_id)

                                ensure_db_dir()
                                conn = sqlite3.connect(DB_PATH)
                                c = conn.cursor()
                                c.execute(
                                    "INSERT INTO processed_events (event_id, event_type, repo, pr_id, created_at) VALUES (?, ?, ?, ?, ?)",
                                    (f"comment_{comment_id}", "comment", repo, pr_id, datetime.now(timezone.utc)))
                                conn.commit()
                                conn.close()

        except Exception as e:
            logger.error(f"Error checking missed PRs for {repo}: {e}")


def periodic_check():
    while True:
        try:
            check_missed_prs()
        except Exception as e:
            logger.error(f"Error in periodic check: {e}")

        time.sleep(3600)


@app.route('/webhook', methods=['POST'])
def handle_webhook():
    global worker_active, worker_last_heartbeat

    if GITHUB_WEBHOOK_SECRET:
        signature = flask.request.headers.get('X-Hub-Signature-256', '')
        payload = flask.request.get_data()
        computed_signature = 'sha256=' + hmac.new(GITHUB_WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, computed_signature):
            logger.warning(f"Invalid webhook signature: {signature}")
            return 'Invalid signature', 403

    event = flask.request.headers.get('X-GitHub-Event')
    payload = flask.request.json

    logger.info(f"Received GitHub event: {event}")

    if event == 'pull_request':
        action = payload['action']
        if action in ['opened', 'synchronize']:
            pr_data = payload['pull_request']
            repo = pr_data['base']['repo']['full_name']

            create_task_from_pr(pr_data, repo, event)

    elif event == 'issue_comment':
        action = payload['action']
        if action == 'created':
            comment = payload['comment']
            comment_id = comment['id']
            comment_body = comment['body']

            if '/run-tests' in comment_body:
                issue = payload['issue']
                if 'pull_request' in issue:
                    pr_url = issue['pull_request']['url']

                    headers = {
                        'Authorization': f'token {GITHUB_TOKEN}',
                        'Accept': 'application/vnd.github.v3+json'
                    }

                    pr_response = requests.get(pr_url, headers=headers, timeout=30)
                    if pr_response.status_code == 200:
                        pr_data = pr_response.json()
                        repo = pr_data['base']['repo']['full_name']

                        create_task_from_pr(pr_data, repo, "comment", comment_id)

                        ensure_db_dir()
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute(
                            "INSERT INTO processed_events (event_id, event_type, repo, pr_id, created_at) VALUES (?, ?, ?, ?, ?)",
                            (f"comment_{comment_id}", "comment", repo, pr_data['number'], datetime.now(timezone.utc)))
                        conn.commit()
                        conn.close()

    return 'OK', 200


@app.route('/api/worker/get_task', methods=['GET'])
def worker_get_task():
    try:
        task = task_queue.get_nowait()

        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Проверяем, не взял ли уже другой воркер эту задачу
        c.execute("SELECT status FROM tasks WHERE id=?", (task['id'],))
        current_status = c.fetchone()

        if current_status and current_status[0] == 'processing':
            # Задача уже в обработке другим воркером, возвращаем её в очередь
            logger.warning(f"⚠️ Задача {task['id']} уже в обработке, возвращаем в очередь")
            task_queue.put(task)
            conn.close()
            return jsonify({'message': 'Task already being processed'}), 409

        # Обновляем статус задачи
        c.execute("UPDATE tasks SET status=?, started_at=? WHERE id=?",
                  ('processing', datetime.now(timezone.utc), task['id']))
        conn.commit()
        conn.close()

        logger.info(f"✅ Задача {task['id']} назначена воркеру")
        return jsonify(task)
    except queue.Empty:
        return jsonify({'message': 'No tasks available'}), 404


@app.route('/api/worker/task_result', methods=['POST'])
def worker_task_result():
    global worker_active

    data = request.json
    logger.info(f"Received task result: {json.dumps(data, ensure_ascii=False)}")

    task_id = data.get('id')
    status = data.get('status')
    result_json = data.get('result_json')
    logs = data.get('logs')

    if not task_id or not status:
        return jsonify({'error': 'Missing task id or status'}), 400

    status_mapping = {
        'success': 'completed',
        'failure': 'failed',
        'error': 'failed',
        'timeout': 'failed'
    }
    task_status = status_mapping.get(status, 'failed')

    result_data = {}
    if result_json:
        try:
            result_data = json.loads(result_json)
            if task_status == 'completed' and (
                    not result_data.get('timings') or not any(result_data['timings'].values())):
                result_data['timings'] = {'device_info_bandwidth': 0.0}
                logger.info(f"Added default zero bandwidth for task {task_id}")
        except json.JSONDecodeError:
            logger.error(f"Failed to parse result_json for task {task_id}")

    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("UPDATE tasks SET status=?, completed_at=? WHERE id=?",
              (task_status, datetime.now(timezone.utc), task_id))

    c.execute("SELECT pr_id, repo, commit_sha, comment_id FROM tasks WHERE id=?", (task_id,))
    task_data = c.fetchone()
    if task_data:
        pr_id, repo, commit_sha, comment_id = task_data
        c.execute(
            "INSERT OR REPLACE INTO tests (id, pr_id, repo, status, result_json, logs, created_at, updated_at, commit_sha, comment_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, pr_id, repo, task_status, json.dumps(result_data), logs, datetime.now(timezone.utc),
             datetime.now(timezone.utc), commit_sha, comment_id))

    conn.commit()
    conn.close()

    comment = generate_comment(result_data, pr_id, repo, task_status, logs)
    post_github_comment(repo, pr_id, comment)

    logger.info(f"Task {task_id} completed with status: {task_status}")

    telegram_message = generate_telegram_message(result_data, pr_id, repo, task_status, logs)
    send_telegram_message(telegram_message)

    return jsonify({'message': 'Result received'})


@app.route('/api/worker/heartbeat', methods=['POST'])
def worker_heartbeat():
    global worker_last_heartbeat

    with worker_lock:
        worker_last_heartbeat = time.time()

    logger.debug("Worker heartbeat received")
    return jsonify({'message': 'Heartbeat received'})


@app.route('/logs/<int:pr_id>')
def view_logs(pr_id):
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT logs, created_at FROM tests WHERE pr_id = ? ORDER BY created_at DESC LIMIT 1", (pr_id,))
    row = c.fetchone()
    conn.close()

    if row:
        logs, created_at = row
        return f"<pre>Logs for PR #{pr_id} ({created_at}):\n\n{logs}</pre>"
    else:
        return "Logs not found for this PR", 404


@app.route('/api/system_status')
def system_status():
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT status, COUNT(*) FROM tests GROUP BY status")
        status_stats = dict(c.fetchall())

        c.execute("SELECT pr_id, repo, status, created_at FROM tests ORDER BY created_at DESC LIMIT 5")
        recent_tasks = []
        for row in c.fetchall():
            recent_tasks.append({
                'pr_id': row[0],
                'repo': row[1],
                'status': row[2],
                'created_at': row[3]
            })

        with worker_lock:
            worker_status = "active" if worker_active else "inactive"
            last_heartbeat = worker_last_heartbeat

        queue_size = task_queue.qsize()

        vm_status = get_vm_status(WORKER_VM_ID)
        worker_ip = get_worker_ip()

        conn.close()

        return jsonify({
            'status_stats': status_stats,
            'recent_tasks': recent_tasks,
            'worker_status': worker_status,
            'last_heartbeat': last_heartbeat,
            'queue_size': queue_size,
            'vm_status': vm_status,
            'worker_ip': worker_ip,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/vm/start', methods=['POST'])
def api_vm_start():
    if not USE_YC_CLOUD:
        return jsonify({'success': True, 'message': 'Yandex Cloud disabled, VM management not available'})

    success = start_worker_vm()
    if success:
        with worker_lock:
            worker_active = True
            worker_last_heartbeat = time.time()
        return jsonify({'success': True, 'message': 'VM started successfully'})
    else:
        return jsonify({'success': False, 'message': 'Failed to start VM'})


@app.route('/api/vm/stop', methods=['POST'])
def api_vm_stop():
    if not USE_YC_CLOUD:
        return jsonify({'success': True, 'message': 'Yandex Cloud disabled, VM management not available'})

    success = stop_worker_vm()
    if success:
        with worker_lock:
            worker_active = False
        return jsonify({'success': True, 'message': 'VM stopped successfully'})
    else:
        return jsonify({'success': False, 'message': 'Failed to stop VM'})


@app.route('/api/vm/status')
def api_vm_status():
    if not USE_YC_CLOUD:
        return jsonify({'success': True, 'status': 'RUNNING', 'message': 'Yandex Cloud disabled'})

    status = get_vm_status(WORKER_VM_ID)
    ip_address = get_worker_ip()
    if status:
        return jsonify({'success': True, 'status': status, 'ip_address': ip_address})
    else:
        return jsonify({'success': False, 'message': 'Failed to get VM status'})


@app.route('/api/check_missed_prs', methods=['POST'])
def api_check_missed_prs():
    try:
        thread = threading.Thread(target=check_missed_prs)
        thread.daemon = True
        thread.start()

        return jsonify({
            'success': True,
            'message': 'Проверка пропущенных PR запущена в фоновом режиме'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Ошибка при запуске проверки: {str(e)}'
        }), 500


@app.route('/api/manual_start/<path:repo>/<int:pr_id>', methods=['POST'])
def manual_start_pr(repo, pr_id):
    try:
        if not GITHUB_TOKEN:
            return jsonify({'error': 'GITHUB_TOKEN not configured'}), 500

        logger.info(f"Manual start requested for PR #{pr_id} in repository {repo}")

        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }

        pr_url = f'https://api.github.com/repos/{repo}/pulls/{pr_id}'
        response = requests.get(pr_url, headers=headers, timeout=30)

        if response.status_code != 200:
            logger.error(f"Failed to get PR info: {response.status_code}")
            return jsonify({'error': f'PR not found: {response.status_code}'}), 404

        pr_data = response.json()

        if pr_data.get('state') != 'open':
            return jsonify({'error': 'PR is not open. Testing can only be performed on open PRs.'}), 400

        # Очищаем старые задачи перед созданием новой
        cleanup_old_tasks(pr_id, repo)

        create_task_from_pr(pr_data, repo, "manual_trigger")

        return jsonify({
            'success': True,
            'message': f'Task created for PR #{pr_id} in {repo}',
            'task_info': {
                'pr_id': pr_id,
                'repo': repo,
                'branch': pr_data['head']['ref'],
                'commit_sha': pr_data['head']['sha'][:8]
            }
        })

    except requests.exceptions.Timeout:
        logger.error(f"Timeout while fetching PR info for {repo}#{pr_id}")
        return jsonify({'error': 'Timeout while fetching PR information from GitHub'}), 504
    except Exception as e:
        logger.error(f"Error in manual start for {repo}#{pr_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/task_status/<path:repo>/<int:pr_id>', methods=['GET'])
def get_task_status(repo, pr_id):
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            SELECT id, status, created_at, started_at, completed_at, restart_count, max_restarts
            FROM tasks
            WHERE pr_id = ? AND repo = ?
            ORDER BY created_at DESC
            LIMIT 1
        """, (pr_id, repo))

        task = c.fetchone()
        conn.close()

        if not task:
            return jsonify({'error': 'No tasks found for this PR'}), 404

        task_id, status, created_at, started_at, completed_at, restart_count, max_restarts = task

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT status, result_json, logs FROM tests WHERE id = ?", (task_id,))
        test_info = c.fetchone()
        conn.close()

        response_data = {
            'pr_id': pr_id,
            'repo': repo,
            'task_id': task_id,
            'status': status,
            'created_at': created_at,
            'started_at': started_at,
            'completed_at': completed_at,
            'restart_count': restart_count,
            'max_restarts': max_restarts,
            'remaining_restarts': max_restarts - restart_count
        }

        if test_info:
            test_status, result_json, logs = test_info
            response_data['test_status'] = test_status
            if result_json:
                try:
                    response_data['result'] = json.loads(result_json)
                except:
                    response_data['result'] = result_json

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Error getting task status for {repo}#{pr_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/queue_status', methods=['GET'])
def get_queue_status():
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
                        SELECT id, pr_id, repo, status, created_at, restart_count, max_restarts
                        FROM tasks
                        WHERE status IN ('pending', 'processing')
                        ORDER BY created_at ASC
                    """)

        queue_tasks = []
        for row in c.fetchall():
            task_id, pr_id, repo, status, created_at, restart_count, max_restarts = row
            queue_tasks.append({
                'task_id': task_id,
                'pr_id': pr_id,
                'repo': repo,
                'status': status,
                'created_at': created_at,
                'restart_count': restart_count,
                'max_restarts': max_restarts,
                'remaining_restarts': max_restarts - restart_count
            })

        c.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        status_stats = dict(c.fetchall())

        conn.close()

        return jsonify({
            'queue_size': task_queue.qsize(),
            'pending_tasks': len([t for t in queue_tasks if t['status'] == 'pending']),
            'processing_tasks': len([t for t in queue_tasks if t['status'] == 'processing']),
            'tasks': queue_tasks,
            'statistics': status_stats
        })

    except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/check_ssh')
def check_ssh():
    if not GPU_WORKER_IP:
        return jsonify({'error': 'GPU_WORKER_IP not configured'}), 500

    success, message = check_ssh_connection(GPU_WORKER_IP)
    return jsonify({'success': success, 'message': message})


@app.route('/api/tasks/<task_id>/restart', methods=['POST'])
def restart_task(task_id):
    try:
        ensure_db_dir()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            SELECT id, pr_id, repo, clone_url, commit_sha, branch, comment_id, restart_count, max_restarts
            FROM tasks WHERE id = ?
        """, (task_id,))

        task = c.fetchone()

        if not task:
            return jsonify({'error': 'Task not found'}), 404

        task_id, pr_id, repo, clone_url, commit_sha, branch, comment_id, restart_count, max_restarts = task

        # Проверяем возможность перезапуска
        if restart_count >= max_restarts:
            return jsonify({
                'success': False,
                'message': f'Достигнут лимит перезапусков ({restart_count}/{max_restarts})'
            }), 400

        # Очищаем старые задачи перед перезапуском (кроме текущей)
        cleanup_old_tasks(pr_id, repo, task_id)

        # Обновляем статус задачи и увеличиваем счетчик перезапусков
        c.execute("""
            UPDATE tasks
            SET status = 'pending', started_at = NULL, completed_at = NULL, restart_count = restart_count + 1
            WHERE id = ?
        """, (task_id,))

        # Возвращаем задачу в очередь
        task_data = {
            'id': task_id,
            'pr_id': pr_id,
            'repo': repo,
            'clone_url': clone_url,
            'commit_sha': commit_sha,
            'branch': branch,
            'comment_id': comment_id
        }
        task_queue.put(task_data)

        conn.commit()
        conn.close()

        logger.info(f"✅ Задача {task_id} перезапущена вручную (попытка {restart_count + 1}/{max_restarts})")
        send_telegram_message(f"🔄 Ручной перезапуск задачи для PR #{pr_id} в {repo} (попытка {restart_count + 1}/{max_restarts})")

        return jsonify({
            'success': True,
            'message': f'Task {task_id} restarted successfully (attempt {restart_count + 1}/{max_restarts})'
        })

    except Exception as e:
        logger.error(f"❌ Ошибка при перезапуске задачи {task_id}: {e}")
        return jsonify({'error': str(e)}), 500


def get_leaderboard():
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
                    SELECT pr_id, repo, result_json
                    FROM tests
                    WHERE (status = 'completed' OR status = 'success_with_warnings') AND result_json IS NOT NULL
                """)
    rows = cur.fetchall()
    conn.close()

    leader_data = []
    for pr_id, repo, result_json in rows:
        try:
            result_data = json.loads(result_json)
            timings = result_data.get('timings', {})
            valid_timings = {k: v for k, v in timings.items() if v > 0}
            total_bandwidth = sum(valid_timings.values()) if valid_timings else 0
            if total_bandwidth > 0:
                username = repo.split('/')[0] if '/' in repo else repo
                leader_data.append({
                    'username': username,
                    'repo': repo,
                    'pr_id': pr_id,
                    'total_bandwidth': total_bandwidth,
                    'timings': valid_timings
                })
        except json.JSONDecodeError:
            continue

    leader_data.sort(key=lambda x: x['total_bandwidth'], reverse=True)
    return leader_data[:10]


@app.route('/leaderboard')
def leaderboard():
    leaderboard_data = get_leaderboard()
    if not leaderboard_data:
        return "Нет данных для отображения", 404

    width = 800
    row_height = max(80, AVATAR_SIZE + 30)
    header_height = 120
    height = header_height + len(leaderboard_data) * row_height

    img = Image.new("RGB", (width, height), (245, 245, 245))
    draw = ImageDraw.Draw(img)

    try:
        font_title = ImageFont.truetype(FONT_PATH, FONT_SIZE_TITLE)
        font_text = ImageFont.truetype(FONT_PATH, FONT_SIZE_TEXT)
        font_small = ImageFont.truetype(FONT_PATH, FONT_SIZE_TEXT - 4)
    except IOError:
        font_title = ImageFont.load_default()
        font_text = ImageFont.load_default()
        font_small = ImageFont.load_default()

    title = "Таблица лидеров по пропускной способности GPU (GB/s)"
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((width - title_width) / 2, 30), title, font=font_title, fill=(0, 0, 0))

    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    time_bbox = draw.textbbox((0, 0), f"Обновлено: {now_str}", font=font_small)
    time_width = time_bbox[2] - time_bbox[0]
    draw.text(((width - time_width) / 2, 80), f"Обновлено: {now_str}", font=font_small, fill=(100, 100, 100))

    draw.line([(50, header_height - 10), (width - 50, header_height - 10)], fill=(200, 200, 200), width=2)

    y = header_height
    for i, entry in enumerate(leaderboard_data, start=1):
        username = entry['username']
        repo = entry['repo']
        pr_id = entry['pr_id']
        total_bandwidth = entry['total_bandwidth']

        if i == 1:
            position_color = GOLD_COLOR
        else:
            position_color = get_user_color(repo)

        row_color = (255, 255, 255) if i % 2 == 1 else (240, 240, 240)
        draw.rectangle([(20, y), (width - 20, y + row_height - 10)], fill=row_color, outline=(220, 220, 220))

        draw.text((40, y + row_height / 2), f"{i}", font=font_title, fill=position_color, anchor="lm")

        avatar = get_avatar(username)
        if avatar:
            img.paste(avatar, (80, y + 10), avatar)

        draw.text((130, y + 15), username, font=font_text, fill=(0, 0, 0))
        draw.text((130, y + 45), repo, font=font_small, fill=(100, 100, 100))

        draw.text((width - 200, y + 15), f"PR #{pr_id}", font=font_small, fill=(100, 100, 100), anchor="rm")

        draw.text((width - 50, y + 15), f"{total_bandwidth:.2f} GB/s", font=font_text, fill=(0, 0, 0), anchor="rm")

        timings_text = ", ".join([f"{k}: {v:.2f} GB/s" for k, v in entry['timings'].items()])
        if timings_text:
            draw.text((width - 50, y + 45), timings_text, font=font_small, fill=(150, 150, 150), anchor="rm")

        y += row_height

    img_io = BytesIO()
    img.save(img_io, 'PNG')
    img_io.seek(0)
    return flask.send_file(img_io, mimetype='image/png')


def get_user_color(repo):
    hash_val = hash(repo) % 0xFFFFFF
    hex_color = f"{hash_val:06x}"
    return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))


def get_avatar(username):
    url = f"{GITHUB_AVATAR_URL}{username}.png?size=80"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            img = Image.open(BytesIO(r.content)).convert("RGBA")
            mask = Image.new('L', (AVATAR_SIZE, AVATAR_SIZE), 0)
            draw = ImageDraw.Draw(mask)
            draw.ellipse((0, 0, AVATAR_SIZE, AVATAR_SIZE), fill=255)
            img = img.resize((AVATAR_SIZE, AVATAR_SIZE), Image.LANCZOS)
            result = Image.new('RGBA', (AVATAR_SIZE, AVATAR_SIZE))
            result.paste(img, (0, 0), mask)
            return result
    except Exception as e:
        logger.error(f"Ошибка загрузки аватарки для {username}: {e}")
    return None


def format_time(seconds):
    if seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    elif seconds < 60:
        return f"{seconds:.2f} s"
    else:
        minutes = seconds / 60
        seconds = seconds % 60
        return f"{minutes:.0f}m {seconds:.2f}s"


def startup_check():
    logger.info("Performing startup checks...")

    if USE_YC_CLOUD:
        if not load_service_account_key():
            logger.error("❌ Не удалось загрузить сервисный ключ Яндекс.Облака")

    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN not set")

    if not WORKER_PASSWORD:
        logger.warning("WORKER_PASSWORD not set")

    if USE_YC_CLOUD and (not WORKER_VM_ID):
        logger.warning("Yandex Cloud configuration missing - VM management will not work")
    else:
        status = get_vm_status()
        ip_address = get_worker_ip()
        logger.info(f"Worker VM status: {status}, IP: {ip_address}")

        if status == "RUNNING":
            with worker_lock:
                worker_active = True
                worker_last_heartbeat = time.time()
            logger.info("Worker VM is running, marking as active")


@app.route('/')
def index():
    return "PR Tester Server is running!"


if __name__ == '__main__':
    ensure_db_dir()

    init_db()
    migrate_db()
    startup_check()

    monitor_thread = threading.Thread(target=worker_monitor, daemon=True)
    monitor_thread.start()

    # Запускаем монитор зависших задач
    stuck_monitor_thread = threading.Thread(target=stuck_task_monitor, daemon=True)
    stuck_monitor_thread.start()

    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)
