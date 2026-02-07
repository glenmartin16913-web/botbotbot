import os
import csv
import aiohttp
import asyncio
import zipfile
import logging
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List, Tuple

from aiohttp import web
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from supabase import create_client, Client as SupabaseClient


# -------------------- LOGGING --------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("bin_bot")

# -------------------- BIN DB --------------------
bin_db: Dict[str, Dict[str, str]] = {}


def load_db() -> bool:
    """Загрузка базы BIN-кодов из ZIP-архива"""
    try:
        csv_path = "full_bins.csv"
        if not os.path.exists(csv_path):
            logger.info("Распаковываю архив full_bins.zip...")
            with zipfile.ZipFile("full_bins.zip", "r") as zip_ref:
                zip_ref.extractall()
            logger.info("Архив успешно распакован")

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                b = (row.get("BIN") or "").strip()
                if b:
                    bin_db[b] = {
                        "Brand": row.get("Brand", "Unknown") or "Unknown",
                        "Issuer": row.get("Issuer", "Unknown") or "Unknown",
                        "CountryName": row.get("CountryName", "Unknown") or "Unknown",
                    }

        logger.info(f"Загружено {len(bin_db)} BIN-кодов")
        return True
    except Exception as e:
        logger.exception(f"Ошибка загрузки базы: {e}")
        return False


def get_card_scheme(bin_code: str) -> str:
    """Определение платёжной системы по BIN-коду"""
    if not bin_code.isdigit() or len(bin_code) < 6:
        return "Unknown"
    first_digit = int(bin_code[0])
    first_two = int(bin_code[:2])
    first_four = int(bin_code[:4])

    if first_digit == 4:
        return "Visa"
    elif 51 <= first_two <= 55 or 2221 <= first_four <= 2720:
        return "MasterCard"
    elif 2200 <= first_four <= 2204:
        return "МИР"
    return "Unknown"


# -------------------- SUPABASE --------------------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
supabase: Optional[SupabaseClient] = None


def init_supabase() -> bool:
    global supabase
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY не заданы!")
        return False
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase клиент инициализирован")
    return True


async def sb_exec(fn, *args, **kwargs):
    """Запуск синхронных supabase-операций в отдельном потоке."""
    return await asyncio.to_thread(fn, *args, **kwargs)


def normalize_username(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower() if u else None


def parse_admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_IDS", "").strip()
    ids = set()
    for part in raw.split(","):
        p = part.strip()
        if p.isdigit():
            ids.add(int(p))
    return ids


ADMIN_IDS = parse_admin_ids()


async def is_allowed_user(user_id: int, username: Optional[str]) -> Tuple[bool, bool]:
    """
    Возвращает (allowed, is_admin).
    Admin = либо в ADMIN_IDS, либо в access_list.role='admin' и is_active=true
    Allowed = либо admin, либо access_list.is_active=true
    """
    if user_id in ADMIN_IDS:
        return True, True

    if supabase is None:
        return False, False

    uname = normalize_username(username)

    def _query_access():
        q = supabase.table("access_list").select("telegram_id, username, role, is_active").limit(1)
        res = q.eq("telegram_id", user_id).execute()
        if res.data:
            return res.data[0]
        if uname:
            res2 = (
                supabase.table("access_list")
                .select("telegram_id, username, role, is_active")
                .limit(1)
                .ilike("username", uname)
                .execute()
            )
            if res2.data:
                return res2.data[0]
        return None

    row = await sb_exec(_query_access)
    if not row:
        return False, False

    if not row.get("is_active", False):
        return False, False

    role = (row.get("role") or "user").lower()
    return True, role == "admin"


async def upsert_user_identity(user_id: int, username: Optional[str]) -> None:
    """
    Если пользователь есть в access_list по username — привяжем telegram_id.
    Если есть по telegram_id — обновим username.
    Ничего не создаём автоматически (бот закрытый).
    """
    if supabase is None:
        return

    uname = normalize_username(username)

    def _work():
        res = (
            supabase.table("access_list")
            .select("id, telegram_id, username")
            .limit(1)
            .eq("telegram_id", user_id)
            .execute()
        )
        if res.data:
            row_id = res.data[0]["id"]
            supabase.table("access_list").update(
                {"username": uname, "updated_at": datetime.utcnow().isoformat()}
            ).eq("id", row_id).execute()
            return

        if uname:
            res2 = (
                supabase.table("access_list")
                .select("id, telegram_id, username")
                .limit(1)
                .ilike("username", uname)
                .execute()
            )
            if res2.data:
                row_id = res2.data[0]["id"]
                supabase.table("access_list").update(
                    {"telegram_id": user_id, "updated_at": datetime.utcnow().isoformat()}
                ).eq("id", row_id).execute()

    await sb_exec(_work)


# -------------------- UI / MENUS --------------------
BTN_BIN = "💳 Проверка карты"
BTN_CP = "👤 Контр агенты"
BTN_ADMIN = "⚙️ Доступ Админ"
BTN_HELP = "ℹ️ Помощь"

MODE_BIN = "bin"
MODE_NONE = "none"


def main_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(BTN_BIN), KeyboardButton(BTN_CP)],
        [KeyboardButton(BTN_HELP)],
    ]
    if is_admin:
        rows.insert(1, [KeyboardButton(BTN_ADMIN)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def cp_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Добавить тег", callback_data="cp:add")],
            [InlineKeyboardButton("⬅️ Назад в меню", callback_data="menu:back")],
        ]
    )


def cp_color_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🟥 Красный", callback_data="cp:color:red"),
                InlineKeyboardButton("🟨 Жёлтый", callback_data="cp:color:yellow"),
                InlineKeyboardButton("🟩 Зелёный", callback_data="cp:color:green"),
            ],
            [InlineKeyboardButton("⬅️ Отмена", callback_data="cp:cancel")],
        ]
    )


def confirm_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"{prefix}:yes"),
                InlineKeyboardButton("❌ Отмена", callback_data=f"{prefix}:no"),
            ]
        ]
    )


def admin_actions_keyboard() -> InlineKeyboardMarkup:
    # Убрали "Список до 30"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Выдать доступ", callback_data="adm:grant")],
            [InlineKeyboardButton("⛔ Забрать доступ", callback_data="adm:revoke")],
            [InlineKeyboardButton("⬅️ Назад в меню", callback_data="menu:back")],
        ]
    )


# -------------------- CLEAN UI HELPERS --------------------
async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def remember_bot_message(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    context.user_data["last_bot_msg_id"] = message_id


async def cleanup_previous_bot_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    mid = context.user_data.get("last_bot_msg_id")
    if mid:
        await safe_delete_message(context, chat_id, mid)
        context.user_data["last_bot_msg_id"] = None


async def safe_edit_or_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
    parse_mode=ParseMode.HTML,
):
    """
    Если callback_query — редактируем одно сообщение.
    Если обычное сообщение — удаляем прошлое "служебное" сообщение бота и отправляем новое.
    """
    if update.callback_query:
        q = update.callback_query
        try:
            await q.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            m = await q.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            await remember_bot_message(context, m.message_id)
    else:
        chat_id = update.effective_chat.id
        await cleanup_previous_bot_message(context, chat_id)
        m = await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        await remember_bot_message(context, m.message_id)


async def try_delete_user_message(update: Update):
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass


# -------------------- BIN MESSAGE DELAY DELETE + AUTO CLEAN --------------------
async def cleanup_previous_bin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Удаляет ПРЕДЫДУЩЕЕ BIN-сообщение пользователя, но оставляет текущее.
    (Текущее удалится при следующем BIN-запросе)
    """
    chat_id = update.effective_chat.id
    prev_id = context.user_data.get("last_bin_msg_id")
    if prev_id:
        await safe_delete_message(context, chat_id, prev_id)

    if update.message:
        context.user_data["last_bin_msg_id"] = update.message.message_id


async def clear_bin_history(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Авточистка: удаляем последнее BIN-сообщение, когда уходим в другие разделы."""
    prev_id = context.user_data.get("last_bin_msg_id")
    if prev_id:
        await safe_delete_message(context, chat_id, prev_id)
    context.user_data["last_bin_msg_id"] = None


# -------------------- CONVERSATION STATES --------------------
CP_WAIT_NAME, CP_WAIT_COLOR, CP_WAIT_COMMENT, CP_WAIT_CONFIRM = range(4)
ADM_WAIT_ACTION, ADM_WAIT_TARGET = range(2)

# -------------------- TIME FORMAT (MSK) --------------------
MSK = ZoneInfo("Europe/Moscow")


def parse_dt_any(dt_val: Any) -> Optional[datetime]:
    """Пытаемся распарсить created_at из Supabase (обычно ISO)."""
    if not dt_val:
        return None
    if isinstance(dt_val, datetime):
        return dt_val
    if isinstance(dt_val, str):
        s = dt_val.strip()
        # supabase часто отдаёт Z в конце
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None


def fmt_msk(dt_val: Any) -> str:
    dt = parse_dt_any(dt_val)
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_msk = dt.astimezone(MSK)
    return dt_msk.strftime("%d.%m.%Y %H:%M МСК")


# -------------------- HELPERS: COUNTERPARTY --------------------
async def fetch_counterparty_tags(counterparty: str, limit: int = 10) -> List[Dict[str, Any]]:
    if supabase is None:
        return []
    key = counterparty.strip()
    if not key:
        return []

    def _work():
        res = (
            supabase.table("counterparty_tags")
            .select("id,counterparty,color,comment,created_by_username,created_by_telegram_id,created_at")
            .ilike("counterparty", key.lower())
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return res.data or []

    return await sb_exec(_work)


def render_counterparty_card(counterparty: str, tags: List[Dict[str, Any]]) -> str:
    """
    Красивое отображение контрагента: статус + счетчики + последние отметки.
    Статус на русском:
      red   -> Высокий риск
      yellow-> Требует внимания
      green -> Можно работать
    """
    cp = counterparty.strip()

    if not tags:
        return (
            "👤 <b>Контрагент</b>\n"
            f"└ <b>{cp}</b>\n\n"
            "🏷️ <b>Статус</b>\n"
            "└ Пока нет отметок.\n\n"
            "ℹ️ Нажми «➕ Добавить тег», чтобы оставить первую отметку."
        )

    counts = {"red": 0, "yellow": 0, "green": 0}
    for t in tags:
        c = (t.get("color") or "").lower()
        if c in counts:
            counts[c] += 1

    priority = {"red": 3, "yellow": 2, "green": 1}
    dominant = max(counts.keys(), key=lambda c: (counts[c], priority[c]))

    if dominant == "red":
        badge = "🟥 <b>Высокий риск</b>"
    elif dominant == "yellow":
        badge = "🟨 <b>Требует внимания</b>"
    else:
        badge = "🟩 <b>Можно работать</b>"

    header = (
        "👤 <b>Контрагент</b>\n"
        f"└ <b>{cp}</b>\n\n"
        "🏷️ <b>Статус</b>\n"
        f"└ {badge}\n\n"
        "📌 <b>Сводка</b>\n"
        f"└ 🟥 <b>{counts['red']}</b>   🟨 <b>{counts['yellow']}</b>   🟩 <b>{counts['green']}</b>\n"
    )

    notes = []
    for t in tags[:7]:
        c = (t.get("color") or "").lower()
        emoji = "🟥" if c == "red" else "🟨" if c == "yellow" else "🟩" if c == "green" else "🏷️"
        author = t.get("created_by_username") or str(t.get("created_by_telegram_id") or "unknown")
        comment = (t.get("comment") or "").strip()
        if len(comment) > 160:
            comment = comment[:160] + "…"
        ts = fmt_msk(t.get("created_at"))
        ts_part = f" <i>({ts})</i>" if ts else ""
        notes.append(f"{emoji} <b>{author}</b> — {comment}{ts_part}")

    return header + "\n<b>Последние отметки</b>\n" + "\n".join(notes)


async def save_counterparty_tag(counterparty: str, color: str, comment: str, by_id: int, by_username: Optional[str]) -> None:
    if supabase is None:
        return

    # сохраняем created_at в UTC (в карточке покажем МСК)
    payload = {
        "counterparty": counterparty.strip().lower(),
        "color": color,
        "comment": comment.strip(),
        "created_by_telegram_id": by_id,
        "created_by_username": normalize_username(by_username),
        "created_at": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
    }

    def _work():
        supabase.table("counterparty_tags").insert(payload).execute()

    await sb_exec(_work)


# -------------------- HELPERS: ACCESS --------------------
async def grant_access(target: str, role: str = "user") -> str:
    if supabase is None:
        return "❌ Supabase не настроен."

    target = target.strip()
    uname = normalize_username(target)
    tid = int(target) if target.isdigit() else None

    if not uname and tid is None:
        return "❌ Укажи @username или telegram_id числом."

    def _work():
        data = {
            "telegram_id": tid,
            "username": uname,
            "role": role,
            "is_active": True,
            "updated_at": datetime.utcnow().isoformat(),
        }

        if tid is not None:
            res = supabase.table("access_list").select("id").limit(1).eq("telegram_id", tid).execute()
            if res.data:
                supabase.table("access_list").update(data).eq("id", res.data[0]["id"]).execute()
                return "updated_by_id"
            supabase.table("access_list").insert(data).execute()
            return "inserted_by_id"

        res2 = supabase.table("access_list").select("id").limit(1).ilike("username", uname).execute()
        if res2.data:
            supabase.table("access_list").update(data).eq("id", res2.data[0]["id"]).execute()
            return "updated_by_username"
        supabase.table("access_list").insert(data).execute()
        return "inserted_by_username"

    status = await sb_exec(_work)
    return f"✅ Доступ выдан ({status})."


async def revoke_access(target: str) -> str:
    if supabase is None:
        return "❌ Supabase не настроен."

    target = target.strip()
    uname = normalize_username(target)
    tid = int(target) if target.isdigit() else None

    if not uname and tid is None:
        return "❌ Укажи @username или telegram_id числом."

    def _work():
        if tid is not None:
            supabase.table("access_list").update(
                {"is_active": False, "updated_at": datetime.utcnow().isoformat()}
            ).eq("telegram_id", tid).execute()
            return
        supabase.table("access_list").update(
            {"is_active": False, "updated_at": datetime.utcnow().isoformat()}
        ).ilike("username", uname).execute()

    await sb_exec(_work)
    return "⛔ Доступ отключён."


# -------------------- ACCESS GATE --------------------
async def gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, bool]:
    user = update.effective_user
    if not user:
        return False, False

    allowed, is_admin = await is_allowed_user(user.id, user.username)

    if allowed:
        await upsert_user_identity(user.id, user.username)

    context.user_data["is_admin"] = is_admin
    return allowed, is_admin


async def deny(update: Update):
    msg = (
        "⛔️ Бот закрытый.\n\n"
        "Чтобы получить доступ напиши одному из администраторов:\n"
        "@GoldExSenior\n"
        "@GoldexDigital\n"
        "@GoldEx69"
    )
    if update.message:
        await update.message.reply_text(msg)
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(msg)


# -------------------- COMMANDS --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return

    # Авточистка BIN при старте (чтобы не висело старое)
    await clear_bin_history(context, update.effective_chat.id)

    context.user_data["mode"] = MODE_BIN

    await safe_edit_or_send(
        update,
        context,
        "Привет! Выбери режим кнопками ниже.\n\n"
        "💳 <b>Проверка карты</b>: отправь первые 6 цифр (BIN)\n"
        "👤 <b>Контр агенты</b>: поиск тега по нику + добавление тега\n"
        + ("⚙️ <b>Доступ</b>: управление доступами (админ)\n" if is_admin else ""),
        reply_markup=main_keyboard(is_admin),
        parse_mode=ParseMode.HTML,
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return

    # Авточистка BIN при входе в помощь
    await clear_bin_history(context, update.effective_chat.id)

    user = update.effective_user
    await safe_edit_or_send(
        update,
        context,
        "ℹ️ <b>Помощь</b>\n\n"
        f"Твой Telegram ID: <code>{user.id}</code>\n"
        f"Твой username: <code>@{user.username or 'нет'}</code>\n\n"
        "💳 Проверка BIN: отправь 6 цифр.\n"
        "👤 Контр агенты: найди контрагента и добавь тег.\n"
        + ("⚙️ Доступ: выдача/забор доступа.\n" if is_admin else ""),
        reply_markup=main_keyboard(is_admin),
        parse_mode=ParseMode.HTML,
    )


# -------------------- MODE SWITCH (REPLY BUTTONS) --------------------
async def on_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return

    text = (update.message.text or "").strip()

    if text == BTN_BIN:
        context.user_data["mode"] = MODE_BIN
        await safe_edit_or_send(
            update,
            context,
            "💳 Режим проверки карты.\nОтправь первые 6 цифр BIN (пример: <code>424242</code>).",
            reply_markup=main_keyboard(is_admin),
            parse_mode=ParseMode.HTML,
        )
        return

    if text == BTN_HELP:
        await help_cmd(update, context)
        return

    if text == BTN_ADMIN:
        await admin_entry(update, context)
        return

    await safe_edit_or_send(
        update,
        context,
        "Выбери действие кнопками меню 🙂",
        reply_markup=main_keyboard(is_admin),
        parse_mode=None,
    )


# -------------------- BIN CHECK --------------------
async def check_card_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return

    mode = context.user_data.get("mode", MODE_BIN)
    text = (update.message.text or "").strip()

    if mode != MODE_BIN:
        await safe_edit_or_send(
            update,
            context,
            "Сейчас ты не в режиме проверки карты.\nНажми «💳 Проверка карты» или «👤 Контр агенты».",
            reply_markup=main_keyboard(is_admin),
            parse_mode=None,
        )
        return

    bin_code = text[:6] if text.isdigit() else ""
    if not bin_code or len(bin_code) < 6:
        await safe_edit_or_send(
            update,
            context,
            "❌ Нужно 6 цифр BIN. Пример: <code>424242</code>",
            reply_markup=main_keyboard(is_admin),
            parse_mode=ParseMode.HTML,
        )
        return

    # ВАЖНО: удаляем предыдущий BIN, текущее сообщение оставляем
    await cleanup_previous_bin_message(update, context)

    brand = get_card_scheme(bin_code)
    issuer = "Unknown"
    country = "Unknown"

    if bin_code in bin_db:
        data = bin_db[bin_code]
        issuer = data.get("Issuer", issuer)
        country = data.get("CountryName", country)
    else:
        try:
            url = f"https://lookup.binlist.net/{bin_code}"
            headers = {"Accept-Version": "3"}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        issuer = data.get("bank", {}).get("name", issuer)
                        country = data.get("country", {}).get("name", country)
        except Exception as e:
            logger.warning(f"BINLIST API error: {e}")

    await safe_edit_or_send(
        update,
        context,
        f"💳 <b>Платёжная система</b>: {brand}\n"
        f"🏦 <b>Банк</b>: {issuer}\n"
        f"🌍 <b>Страна</b>: {country}",
        reply_markup=main_keyboard(is_admin),
        parse_mode=ParseMode.HTML,
    )


# -------------------- COUNTERPARTY FLOW --------------------
async def cp_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    # Авточистка BIN при входе в контрагентов
    await clear_bin_history(context, update.effective_chat.id)

    context.user_data["mode"] = MODE_NONE
    await safe_edit_or_send(
        update,
        context,
        "👤 <b>Контр агенты</b>\n\nОтправь имя контрагента (ник) как на бирже:",
        reply_markup=main_keyboard(is_admin),
        parse_mode=ParseMode.HTML,
    )
    return CP_WAIT_NAME


async def cp_receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, _ = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    cp = (update.message.text or "").strip()
    if not cp:
        await safe_edit_or_send(update, context, "Напиши ник контрагента текстом.", parse_mode=None)
        return CP_WAIT_NAME

    context.user_data["cp_name"] = cp
    await try_delete_user_message(update)

    tags = await fetch_counterparty_tags(cp, limit=10)
    text = render_counterparty_card(cp, tags)

    await safe_edit_or_send(
        update,
        context,
        text,
        reply_markup=cp_actions_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    return CP_WAIT_NAME


async def cp_add_tag_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, _ = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()

    await safe_edit_or_send(
        update,
        context,
        "Выбери цвет тега:",
        reply_markup=cp_color_keyboard(),
        parse_mode=None,
    )
    return CP_WAIT_COLOR


async def cp_color_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, _ = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()

    _, _, color = q.data.split(":")
    context.user_data["cp_color"] = color

    await safe_edit_or_send(
        update,
        context,
        "Напиши комментарий для этого тега:",
        reply_markup=None,
        parse_mode=None,
    )
    return CP_WAIT_COMMENT


async def cp_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, _ = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    comment = (update.message.text or "").strip()
    if len(comment) < 2:
        await safe_edit_or_send(update, context, "Комментарий слишком короткий. Напиши чуть подробнее.", parse_mode=None)
        return CP_WAIT_COMMENT

    context.user_data["cp_comment"] = comment
    cp = context.user_data.get("cp_name", "")
    color = context.user_data.get("cp_color", "yellow")

    await try_delete_user_message(update)

    emoji = "🟥" if color == "red" else "🟨" if color == "yellow" else "🟩"
    await safe_edit_or_send(
        update,
        context,
        "Проверь и подтверди:\n\n"
        f"Контрагент: <b>{cp}</b>\n"
        f"Тег: {emoji} <b>{color}</b>\n"
        f"Комментарий: <i>{comment}</i>",
        reply_markup=confirm_keyboard("cp:confirm"),
        parse_mode=ParseMode.HTML,
    )
    return CP_WAIT_CONFIRM


async def cp_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, _ = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()

    decision = q.data.split(":")[-1]
    if decision == "no":
        await safe_edit_or_send(
            update,
            context,
            "Ок, отменено. Можешь снова нажать «➕ Добавить тег» или вернуться в меню.",
            reply_markup=cp_actions_keyboard(),
            parse_mode=None,
        )
        return CP_WAIT_NAME

    cp = context.user_data.get("cp_name", "").strip()
    color = context.user_data.get("cp_color", "yellow")
    comment = context.user_data.get("cp_comment", "").strip()

    user = update.effective_user
    await save_counterparty_tag(cp, color, comment, user.id, user.username)

    tags = await fetch_counterparty_tags(cp, limit=10)
    text = "✅ <b>Сохранено</b>\n\n" + render_counterparty_card(cp, tags)

    await safe_edit_or_send(
        update,
        context,
        text,
        reply_markup=cp_actions_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    return CP_WAIT_NAME


async def cp_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()

    await safe_edit_or_send(
        update,
        context,
        "Ок, отменено.",
        reply_markup=main_keyboard(is_admin),
        parse_mode=None,
    )
    return CP_WAIT_NAME


async def back_to_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()

    context.user_data["mode"] = MODE_BIN
    await safe_edit_or_send(
        update,
        context,
        "⬅️ Возврат в меню. Режим проверки карты активен.",
        reply_markup=main_keyboard(is_admin),
        parse_mode=None,
    )
    return ConversationHandler.END


# -------------------- ADMIN ACCESS FLOW --------------------
async def admin_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END
    if not is_admin:
        await safe_edit_or_send(update, context, "⛔ Эта функция доступна только администраторам.", parse_mode=None)
        return ConversationHandler.END

    # Авточистка BIN при входе в админку
    await clear_bin_history(context, update.effective_chat.id)

    context.user_data["mode"] = MODE_NONE

    await safe_edit_or_send(
        update,
        context,
        "⚙️ <b>Доступ</b>\nВыбери действие:",
        reply_markup=admin_actions_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    return ADM_WAIT_ACTION


async def admin_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END
    if not is_admin:
        await deny(update)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()

    action = q.data.split(":")[-1]
    context.user_data["adm_action"] = action

    await safe_edit_or_send(
        update,
        context,
        "Введи @username (без пробелов) или telegram_id числом:",
        reply_markup=None,
        parse_mode=None,
    )
    return ADM_WAIT_TARGET


async def admin_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allowed, is_admin = await gate(update, context)
    if not allowed:
        await deny(update)
        return ConversationHandler.END
    if not is_admin:
        await deny(update)
        return ConversationHandler.END

    target = (update.message.text or "").strip()
    action = context.user_data.get("adm_action")

    await try_delete_user_message(update)

    if action == "grant":
        msg = await grant_access(target, role="user")
        await safe_edit_or_send(update, context, msg, reply_markup=admin_actions_keyboard(), parse_mode=None)
        return ADM_WAIT_ACTION

    if action == "revoke":
        msg = await revoke_access(target)
        await safe_edit_or_send(update, context, msg, reply_markup=admin_actions_keyboard(), parse_mode=None)
        return ADM_WAIT_ACTION

    await safe_edit_or_send(update, context, "Не понял действие. Вернись в меню «Доступ» и выбери снова.", parse_mode=None)
    return ConversationHandler.END


# -------------------- HTTP HEALTH --------------------
async def health_check(request):
    return web.Response(text="OK", status=200)


async def run_http_server(port: int):
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"HTTP-сервер запущен на порту {port}")
    return runner


# -------------------- BOT RUN --------------------
async def run_bot():
    if not load_db():
        logger.critical("Не удалось загрузить базу BIN-кодов!")
        return

    if not init_supabase():
        logger.critical("Supabase не настроен. Проверь SUPABASE_URL и SUPABASE_SERVICE_ROLE_KEY.")
        return

    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        logger.error("TELEGRAM_TOKEN не найден!")
        return

    # Мягко сбросим вебхук (на всякий)
    try:
        temp_app = Application.builder().token(token).build()
        await temp_app.bot.delete_webhook(drop_pending_updates=True)
        await temp_app.shutdown()
        await asyncio.sleep(0.5)
    except Exception:
        pass

    port = int(os.environ.get("PORT", 8080))
    http_runner = await run_http_server(port)

    application = Application.builder().token(token).concurrent_updates(False).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))

    # Conversation: Контр агенты
    cp_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(rf"^{re.escape(BTN_CP)}$"), cp_entry)],
        states={
            CP_WAIT_NAME: [
                CallbackQueryHandler(cp_add_tag_cb, pattern=r"^cp:add$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^menu:back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, cp_receive_name),
            ],
            CP_WAIT_COLOR: [
                CallbackQueryHandler(cp_color_cb, pattern=r"^cp:color:(red|yellow|green)$"),
                CallbackQueryHandler(cp_cancel_cb, pattern=r"^cp:cancel$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^menu:back$"),
            ],
            CP_WAIT_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cp_comment),
            ],
            CP_WAIT_CONFIRM: [
                CallbackQueryHandler(cp_confirm_cb, pattern=r"^cp:confirm:(yes|no)$"),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(cp_conv)

    # Conversation: Админ доступ (ловим разные варианты текста)
    adm_entry_pattern = rf"^({re.escape(BTN_ADMIN)}|⚙️\s*Доступ\s*\(админ\)|Доступ\s*Админ)$"
    adm_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(adm_entry_pattern), admin_entry)],
        states={
            ADM_WAIT_ACTION: [
                CallbackQueryHandler(admin_action_cb, pattern=r"^adm:(grant|revoke)$"),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^menu:back$"),
            ],
            ADM_WAIT_TARGET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_target),
                CallbackQueryHandler(back_to_menu_cb, pattern=r"^menu:back$"),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    application.add_handler(adm_conv)

    # Кнопки меню BIN/HELP/ADMIN (CP ловит conversation entry)
    menu_pattern = rf"^({re.escape(BTN_BIN)}|{re.escape(BTN_HELP)}|{re.escape(BTN_ADMIN)})$"
    application.add_handler(MessageHandler(filters.Regex(menu_pattern), on_menu_button))

    # BIN-чек
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, check_card_message))

    logger.info("Бот запускается...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
    finally:
        logger.info("Остановка бота...")
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        await http_runner.cleanup()
        logger.info("Бот успешно остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Бот остановлен по запросу пользователя")
    except Exception as e:
        logger.exception(f"Фатальная ошибка: {e}")
