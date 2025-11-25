# utils.py
import random
import time
import requests
import os

def choose_user_agent(user_agents):
    return random.choice(user_agents) if user_agents else "Mozilla/5.0 (X11; Linux x86_64)"

def retry_backoff(fn, retries=3, base=1):
    for attempt in range(1, retries+1):
        try:
            return fn()
        except Exception:
            if attempt == retries:
                raise
            time.sleep(base * (2 ** (attempt-1)))

def send_telegram(token, chat_id, text):
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        r = requests.post(url, data=payload, timeout=15)
        return r.ok
    except Exception:
        return False
