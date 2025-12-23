import os
import sqlite3
import logging
from typing import Optional, Dict, Any, List, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    ContextTypes,
    CommandHandler,
)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration/constants ---
DB_FILE = "users.db"
ADMIN_DUMP_USER_ID = 1637269136  

# States for ConversationHandler
POSITION, MODE, MMR, SEARCH_MODE, SEARCH_POS_OPTION, SELECT_POSITION, SEARCH_FULL_OPTION, SEARCH_MMR = range(8)

POSITIONS = {
    "1": "Carry",
    "2": "Mid",
    "3": "Offlane",
    "4": "Soft Support",
    "5": "Hard Support",
}

GAME_MODES = ["Turbo", "All Pick", "Single Draft", "Ranked"]


# --- Database utilities ---


def init_db():
    """
    Create table if needed and migrate schema to include new columns (mode, mmr, username, online, full_party).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS profiles (
            user_id INTEGER PRIMARY KEY,
            position TEXT,
            mode TEXT,
            mmr INTEGER,
            username TEXT,
            online INTEGER DEFAULT 0,
            full_party INTEGER DEFAULT 0
        )
        """
    )
    conn.commit()

    # Migration: ensure columns exist (in case table was created before adding columns)
    cursor.execute("PRAGMA table_info(profiles)")
    cols = {row[1] for row in cursor.fetchall()}
    if "mode" not in cols:
        try:
            cursor.execute("ALTER TABLE profiles ADD COLUMN mode TEXT")
        except Exception:
            pass
    if "mmr" not in cols:
        try:
            cursor.execute("ALTER TABLE profiles ADD COLUMN mmr INTEGER")
        except Exception:
            pass
    if "username" not in cols:
        try:
            cursor.execute("ALTER TABLE profiles ADD COLUMN username TEXT")
        except Exception:
            pass
    if "online" not in cols:
        try:
            cursor.execute("ALTER TABLE profiles ADD COLUMN online INTEGER DEFAULT 0")
            cursor.execute("UPDATE profiles SET online = 0 WHERE online IS NULL")
        except Exception:
            pass
    if "full_party" not in cols:
        try:
            cursor.execute("ALTER TABLE profiles ADD COLUMN full_party INTEGER DEFAULT 0")
            cursor.execute("UPDATE profiles SET full_party = 0 WHERE full_party IS NULL")
        except Exception:
            pass

    conn.commit()
    conn.close()


def get_profile(user_id: int) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_id, position, mode, mmr, username, online, full_party FROM profiles WHERE user_id = ?",
        (user_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "user_id": row[0],
        "position": row[1],
        "mode": row[2],
        "mmr": row[3],
        "username": row[4],
        "online": bool(row[5]) if row[5] is not None else False,
        "full_party": bool(row[6]) if row[6] is not None else False,
    }


def upsert_profile(
    user_id: int,
    position: Optional[str] = None,
    mode: Optional[str] = None,
    mmr: Optional[int] = None,
    username: Optional[str] = None,
    online: Optional[int] = None,
    full_party: Optional[int] = None,
):
    """
    Insert or update profile fields provided (keeps other fields intact).
    online/full_party: 1 or 0 or None (if None, don't change)
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM profiles WHERE user_id = ?", (user_id,))
    exists = cursor.fetchone() is not None

    if exists:
        fields = []
        params = []
        if position is not None:
            fields.append("position = ?")
            params.append(position)
        if mode is not None:
            fields.append("mode = ?")
            params.append(mode)
        if mmr is not None:
            fields.append("mmr = ?")
            params.append(mmr)
        if username is not None:
            fields.append("username = ?")
            params.append(username)
        if online is not None:
            fields.append("online = ?")
            params.append(1 if online else 0)
        if full_party is not None:
            fields.append("full_party = ?")
            params.append(1 if full_party else 0)
        if fields:
            params.append(user_id)
            sql = f"UPDATE profiles SET {', '.join(fields)} WHERE user_id = ?"
            cursor.execute(sql, params)
    else:
        insert_online = 1 if online else 0
        insert_full = 1 if full_party else 0
        cursor.execute(
            "INSERT INTO profiles (user_id, position, mode, mmr, username, online, full_party) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, position, mode, mmr, username, insert_online, insert_full),
        )
    conn.commit()
    conn.close()


# Initialize DB (and perform migrations if required)
init_db()


# --- Keyboards / UI helpers ---


def back_and_menu_row():
    return [InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
            InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]


def get_main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Искать тиммейта", callback_data="search_party")],
        [InlineKeyboardButton("👤 Мой профиль", callback_data="my_profile")],
    ])


def profile_edit_keyboard_dynamic(is_online: bool, full_party: bool):
    online_label = "🟢 Онлайн" if is_online else "⚪ Офлайн"
    full_label = "🤝 Full: ON" if full_party else "🤝 Full: OFF"
    kb = [
        [InlineKeyboardButton(online_label, callback_data="toggle_online"),
         InlineKeyboardButton(full_label, callback_data="toggle_fullparty")],
        [InlineKeyboardButton("✏️ Позиция", callback_data="edit_position"),
         InlineKeyboardButton("✏️ Режим", callback_data="edit_mode")],
        [InlineKeyboardButton("✏️ MMR", callback_data="edit_mmr")],
        back_and_menu_row()
    ]
    return InlineKeyboardMarkup(kb)


def mode_selection_keyboard(action_prefix="mode_", include_back=True):
    keyboard = [[InlineKeyboardButton(m, callback_data=f"{action_prefix}{m}")] for m in GAME_MODES]
    if include_back:
        keyboard.append(back_and_menu_row())
    else:
        keyboard.append([InlineKeyboardButton("🏠 В меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(keyboard)


def search_pos_option_keyboard_dynamic(context: Dict[str, Any]):
    exclude = context.get("exclude_position")
    if exclude is None:
        exclude = True  # default ON
    label = f"🚫 Искл. мою поз.: {'ON' if exclude else 'OFF'}"
    keyboard = [
        [InlineKeyboardButton(label, callback_data="toggle_exclude_position"),
         InlineKeyboardButton("▶️ Начать поиск", callback_data="start_search")],
        [InlineKeyboardButton("🔎 Указать позицию", callback_data="spec_position")],
        back_and_menu_row()
    ]
    return InlineKeyboardMarkup(keyboard)


def select_position_keyboard():
    keyboard = [
        [InlineKeyboardButton("1 — Carry", callback_data="selectpos_1"),
         InlineKeyboardButton("2 — Mid", callback_data="selectpos_2")],
        [InlineKeyboardButton("3 — Offlane", callback_data="selectpos_3"),
         InlineKeyboardButton("4 — Soft Support", callback_data="selectpos_4")],
        [InlineKeyboardButton("5 — Hard Support", callback_data="selectpos_5")],
        back_and_menu_row()
    ]
    return InlineKeyboardMarkup(keyboard)


def search_full_option_keyboard(include_back=True):
    keyboard = [
        [InlineKeyboardButton("🔒 Только Full: Да", callback_data="only_full_yes"),
         InlineKeyboardButton("🔓 Только Full: Нет", callback_data="only_full_no")]
    ]
    if include_back:
        keyboard.append(back_and_menu_row())
    else:
        keyboard.append([InlineKeyboardButton("🏠 В меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(keyboard)


def search_mmr_keyboard():
    keyboard = [
        [InlineKeyboardButton("Не учитывать MMR", callback_data="mmr_none")],
        [InlineKeyboardButton("Δ 100", callback_data="delta_100"), InlineKeyboardButton("Δ 250", callback_data="delta_250")],
        [InlineKeyboardButton("Δ 500", callback_data="delta_500"), InlineKeyboardButton("Указать вручную", callback_data="delta_custom")],
        back_and_menu_row()
    ]
    return InlineKeyboardMarkup(keyboard)


# --- Navigation (back stack) helpers ---


def push_back(context: ContextTypes.DEFAULT_TYPE, prev: str):
    stack = context.user_data.get("back_stack", [])
    stack.append(prev)
    context.user_data["back_stack"] = stack


def pop_back(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    stack = context.user_data.get("back_stack", [])
    if not stack:
        return None
    val = stack.pop()
    context.user_data["back_stack"] = stack
    return val


def clear_back(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("back_stack", None)


# store/retrieve last shown text for steps (for restoring on Back)
def store_last_text(context: ContextTypes.DEFAULT_TYPE, step: str, text: str):
    context.user_data[f"last_text_{step}"] = text


def get_last_text(context: ContextTypes.DEFAULT_TYPE, step: str) -> Optional[str]:
    return context.user_data.get(f"last_text_{step}")


# --- UI rendering on Back ---


async def render_prev(prev: Optional[str], update_obj, context: ContextTypes.DEFAULT_TYPE):
    send = None
    edit = None
    from_user_id = None
    if hasattr(update_obj, "answer"):  # CallbackQuery
        cq = update_obj
        edit = cq.edit_message_text
        from_user_id = cq.from_user.id
    else:
        send = update_obj.message.reply_text
        from_user_id = update_obj.message.from_user.id

    async def respond(text: str, reply_markup=None):
        if edit:
            await edit(text=text, reply_markup=reply_markup)
        else:
            await send(text, reply_markup=reply_markup)

    if not prev:
        await respond("Главное меню:", reply_markup=get_main_keyboard())
        return

    if prev == "MAIN_MENU":
        clear_back(context)
        await respond("Главное меню:", reply_markup=get_main_keyboard())
        return

    if prev == "PROFILE":
        profile = get_profile(from_user_id)
        online = profile["online"] if profile else False
        full = profile["full_party"] if profile else False
        last = get_last_text(context, "PROFILE")
        text = last or (
            "👤 Твой профиль:\n\n"
            f"🎯 Позиция: {profile['position'] if profile else '—'}\n"
            f"🎮 Предпочитаемый режим: {profile['mode'] if profile else '—'}\n"
            f"📊 MMR: {profile['mmr'] if profile and profile['mmr'] is not None else '—'}\n"
            (f"🔗 Username: @{profile['username']}\n" if profile and profile.get("username") else "") +
            "\n\nСтатус Online/Offline:\n"
            "Если вы включите Online — вас будут показывать в результатах поиска и вам могут написать.\n"
            "Если выключите — вы не будете видны в поиске и вас не будут беспокоить."
        )
        await respond(text, reply_markup=profile_edit_keyboard_dynamic(online, full))
        return

    if prev == "SEARCH_MODE":
        last = get_last_text(context, "SEARCH_MODE")
        text = last or "Выбери режим игры для поиска:"
        await respond(text, reply_markup=mode_selection_keyboard(action_prefix="mode_"))
        return

    if prev == "SEARCH_POS_OPTION":
        last = get_last_text(context, "SEARCH_POS_OPTION")
        text = last or "Хотите исключать вашу позицию при поиске, или искать определённую позицию?"
        await respond(text, reply_markup=search_pos_option_keyboard_dynamic(context.user_data))
        return

    if prev == "SELECT_POSITION":
        last = get_last_text(context, "SELECT_POSITION")
        text = last or "Выберите позицию для поиска:"
        await respond(text, reply_markup=select_position_keyboard())
        return

    if prev == "SEARCH_FULL_OPTION":
        last = get_last_text(context, "SEARCH_FULL_OPTION")
        text = last or "Искать только тех, кто согласен на Full Party?"
        await respond(text, reply_markup=search_full_option_keyboard())
        return

    if prev == "SEARCH_MMR":
        last = get_last_text(context, "SEARCH_MMR")
        text = last or "Теперь выберите опции по MMR:"
        await respond(text, reply_markup=search_mmr_keyboard())
        return

    await respond("Главное меню:", reply_markup=get_main_keyboard())


# --- Handlers ---


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        clear_back(context)
        await update.message.reply_text(
            "Привет! Это бот для поиска пати в Dota 2 🔥\nВыбери действие:",
            reply_markup=get_main_keyboard()
        )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # Back
    if data == "go_back":
        prev = pop_back(context)
        await render_prev(prev, query, context)
        return ConversationHandler.END

    # Main menu
    if data == "main_menu":
        clear_back(context)
        context.user_data.pop("search_mode", None)
        context.user_data.pop("exclude_position", None)
        context.user_data.pop("specific_position", None)
        context.user_data.pop("only_full_party", None)
        context.user_data.pop("own_position", None)
        await query.edit_message_text("Главное меню:", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    # Profile view
    if data == "my_profile":
        profile = get_profile(user_id)
        pos = profile["position"] if profile else None
        mode = profile["mode"] if profile else None
        mmr = profile["mmr"] if profile else None
        username = profile["username"] if profile else None
        online = profile["online"] if profile else False
        full_party = profile["full_party"] if profile else False

        text = "👤 Твой профиль:\n\n"
        text += f"🎯 Позиция: {pos or '—'}\n"
        text += f"🎮 Предпочитаемый режим: {mode or '—'}\n"
        text += f"📊 MMR: {mmr if mmr is not None else '—'}\n"
        text += f"🔗 Username: @{username}\n" if username else ""
        text += (
            "\n\nСтатус Online/Offline:\n"
            "Если вы включите Online — вас будут показывать в результатах поиска и вам могут написать.\n"
            "Если выключите — вы не будете видны в поиске и вас не будут беспокоить."
        )
        text += (
            "\n\nFull party (согласен на полную пати):\n"
            "Если включено — вы помечены как готовый играть в полную пати."
        )
        store_last_text(context, "PROFILE", text)
        await query.edit_message_text(text=text, reply_markup=profile_edit_keyboard_dynamic(online, full_party))
        return ConversationHandler.END

    # Toggle online
    if data == "toggle_online":
        profile = get_profile(user_id) or {}
        current_online = profile.get("online", False)
        new_online = not current_online
        username = query.from_user.username
        try:
            upsert_profile(user_id=user_id, username=username, online=1 if new_online else 0)
        except Exception as e:
            logger.error("Error toggling online: %s", e)
            await query.edit_message_text("Ошибка при переключении статуса. Попробуйте позже.", reply_markup=get_main_keyboard())
            return ConversationHandler.END

        if new_online:
            text = "🟢 Вы включили статус ONLINE.\n\nЭто означает: люди будут видеть вас в результатах поиска и смогут написать."
        else:
            text = "⚪ Вы переключились в OFFLINE.\n\nВы не будете видны в поиске и вас не будут беспокоить."
        profile = get_profile(user_id)
        full_party = profile["full_party"] if profile else False
        store_last_text(context, "PROFILE", text)
        await query.edit_message_text(text=text, reply_markup=profile_edit_keyboard_dynamic(new_online, full_party))
        return ConversationHandler.END

    # Toggle full_party
    if data == "toggle_fullparty":
        profile = get_profile(user_id) or {}
        current = profile.get("full_party", False)
        new = not current
        username = query.from_user.username
        try:
            upsert_profile(user_id=user_id, username=username, full_party=1 if new else 0)
        except Exception as e:
            logger.error("Error toggling full_party: %s", e)
            await query.edit_message_text("Ошибка при переключении опции. Попробуйте позже.", reply_markup=get_main_keyboard())
            return ConversationHandler.END

        if new:
            text = "✅ Вы включили согласие на Full Party.\n\nЭто показывает другим, что вы согласны играть в полную пати."
        else:
            text = "❌ Вы отключили согласие на Full Party.\n\nВы не помечены как желающий играть в полную пати."
        profile = get_profile(user_id)
        online = profile["online"] if profile else False
        store_last_text(context, "PROFILE", text)
        await query.edit_message_text(text=text, reply_markup=profile_edit_keyboard_dynamic(online, new))
        return ConversationHandler.END

    # Edit profile flows
    if data == "edit_position":
        push_back(context, "PROFILE")
        text = "Укажи свою предпочитаемую позицию цифрой:\n\n1 — Carry\n2 — Mid\n3 — Offlane\n4 — Soft Support\n5 — Hard Support"
        store_last_text(context, "POSITION", text)
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
                                                                                InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]]))
        return POSITION

    if data == "edit_mode":
        push_back(context, "PROFILE")
        text = "Выбери предпочитаемый режим (будет сохранён в профиле):"
        store_last_text(context, "MODE", text)
        await query.edit_message_text(text, reply_markup=mode_selection_keyboard(action_prefix="setmode_"))
        return MODE

    if data == "edit_mmr":
        push_back(context, "PROFILE")
        text = "Введи свой MMR (целое число, например: 3500):"
        store_last_text(context, "MMR", text)
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
                                                                                InlineKeyboardButton("🏠 В меню", callback_data="main_menu"),
                                                                                InlineKeyboardButton("Отмена", callback_data="go_back")]]))
        return MMR

    # Save preferred mode in profile
    if data.startswith("setmode_"):
        mode_name = data[len("setmode_"):]
        username = query.from_user.username
        try:
            upsert_profile(user_id=user_id, mode=mode_name, username=username)
            profile = get_profile(user_id)
            online = profile["online"] if profile else False
            full_party = profile["full_party"] if profile else False
            text = f"✅ Предпочитаемый режим сохранён: {mode_name}"
            store_last_text(context, "PROFILE", text)
            await query.edit_message_text(text, reply_markup=profile_edit_keyboard_dynamic(online, full_party))
        except Exception as e:
            logger.error("Error saving profile mode: %s", e)
            await query.edit_message_text("Ошибка сохранения режима. Попробуйте позже.", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    # Start search flow
    if data == "search_party":
        profile = get_profile(user_id)
        if not profile or not profile.get("position"):
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📝 Указать позицию", callback_data="edit_position")],
                                             [InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]])
            await query.edit_message_text("❌ Сначала укажи позицию в профиле!", reply_markup=keyboard)
            return ConversationHandler.END

        context.user_data["own_position"] = profile["position"]
        # define default for exclude_position to avoid undefined behavior
        context.user_data.setdefault("exclude_position", True)
        context.user_data.pop("specific_position", None)
        context.user_data.pop("only_full_party", None)
        clear_back(context)
        push_back(context, "MAIN_MENU")
        text = "Выбери режим игры для поиска:"
        store_last_text(context, "SEARCH_MODE", text)
        await query.edit_message_text(text, reply_markup=mode_selection_keyboard(action_prefix="mode_"))
        return SEARCH_MODE

    # Mode chosen for searching (strict)
    if data.startswith("mode_"):
        mode_name = data[len("mode_"):]
        search_pos = context.user_data.get("own_position")
        if not search_pos:
            await query.edit_message_text("Сначала выберите позицию в профиле.", reply_markup=get_main_keyboard())
            return ConversationHandler.END

        push_back(context, "SEARCH_MODE")
        context.user_data["search_mode"] = mode_name
        context.user_data.setdefault("exclude_position", True)
        text = "Хотите исключать вашу позицию при поиске, или искать определённую позицию?"
        store_last_text(context, "SEARCH_POS_OPTION", text)
        await query.edit_message_text(text, reply_markup=search_pos_option_keyboard_dynamic(context.user_data))
        return SEARCH_POS_OPTION

    # Toggle exclude - toggle in-place, do not advance
    if data == "toggle_exclude_position":
        cur = context.user_data.get("exclude_position")
        new = not (cur if cur is not None else True)
        context.user_data["exclude_position"] = new
        logger.info("User %s toggled exclude_position -> %s", user_id, new)
        text = "Выберите действие по позиции для поиска:"
        store_last_text(context, "SEARCH_POS_OPTION", text)
        await query.edit_message_text(text, reply_markup=search_pos_option_keyboard_dynamic(context.user_data))
        return ConversationHandler.END

    # Start search button from pos options
    if data == "start_search":
        push_back(context, "SEARCH_POS_OPTION")
        text_full = "Искать только тех, кто согласен на Full Party?"
        store_last_text(context, "SEARCH_FULL_OPTION", text_full)
        await query.edit_message_text(text_full, reply_markup=search_full_option_keyboard())
        return SEARCH_FULL_OPTION

    if data == "spec_position":
        push_back(context, "SEARCH_POS_OPTION")
        text = "Выберите позицию для поиска:"
        store_last_text(context, "SELECT_POSITION", text)
        await query.edit_message_text(text, reply_markup=select_position_keyboard())
        return SELECT_POSITION

    if data.startswith("selectpos_"):
        key = data.split("_", 1)[1]
        if key not in POSITIONS:
            await query.edit_message_text("Неверный выбор позиции.", reply_markup=get_main_keyboard())
            return ConversationHandler.END
        pos_name = POSITIONS[key]
        context.user_data["specific_position"] = pos_name
        context.user_data.pop("exclude_position", None)
        push_back(context, "SELECT_POSITION")
        text_full = "Искать только тех, кто согласен на Full Party?"
        store_last_text(context, "SEARCH_FULL_OPTION", text_full)
        await query.edit_message_text(text_full, reply_markup=search_full_option_keyboard())
        return SEARCH_FULL_OPTION

    # Full party options
    if data == "only_full_yes":
        context.user_data["only_full_party"] = True
        push_back(context, "SEARCH_FULL_OPTION")
        text = "Выбрано: только Full party. Теперь выберите опции по MMR:"
        store_last_text(context, "SEARCH_MMR", "Теперь выберите опции по MMR:")
        await query.edit_message_text(text, reply_markup=search_mmr_keyboard())
        return SEARCH_MMR

    if data == "only_full_no":
        context.user_data["only_full_party"] = False
        push_back(context, "SEARCH_FULL_OPTION")
        text = "Выбрано: не фильтровать по Full party. Теперь выберите опции по MMR:"
        store_last_text(context, "SEARCH_MMR", "Теперь выберите опции по MMR:")
        await query.edit_message_text(text, reply_markup=search_mmr_keyboard())
        return SEARCH_MMR

    # MMR options - start search
    if data == "mmr_none":
        search_mode = context.user_data.get("search_mode")
        exclude_pos = context.user_data.get("exclude_position", True)
        specific_pos = context.user_data.get("specific_position")
        only_full = context.user_data.get("only_full_party", False)
        logger.info("Starting search: user=%s mode=%s exclude_pos=%s specific_pos=%s only_full=%s mmr_filter=None", user_id, search_mode, exclude_pos, specific_pos, only_full)
        await perform_search_and_reply(query, user_id, search_mode, mmr_filter=None, exclude_position=exclude_pos, specific_position=specific_pos, only_full_party=only_full)
        context.user_data.pop("search_mode", None)
        context.user_data.pop("exclude_position", None)
        context.user_data.pop("specific_position", None)
        context.user_data.pop("only_full_party", None)
        context.user_data.pop("own_position", None)
        clear_back(context)
        return ConversationHandler.END

    if data.startswith("delta_"):
        delta = int(data.split("_", 1)[1])
        profile = get_profile(user_id)
        user_mmr = profile.get("mmr") if profile else None
        if user_mmr is None:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📝 Указать MMR", callback_data="edit_mmr")],
                                            [InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]])
            await query.edit_message_text("Чтобы фильтровать по MMR, сначала укажи свой MMR.", reply_markup=keyboard)
            return ConversationHandler.END

        search_mode = context.user_data.get("search_mode")
        exclude_pos = context.user_data.get("exclude_position", True)
        specific_pos = context.user_data.get("specific_position")
        only_full = context.user_data.get("only_full_party", False)
        logger.info("Starting search: user=%s mode=%s exclude_pos=%s specific_pos=%s only_full=%s mmr_filter=%s", user_id, search_mode, exclude_pos, specific_pos, only_full, delta)
        await perform_search_and_reply(query, user_id, search_mode, mmr_filter=delta, exclude_position=exclude_pos, specific_position=specific_pos, only_full_party=only_full)
        context.user_data.pop("search_mode", None)
        context.user_data.pop("exclude_position", None)
        context.user_data.pop("specific_position", None)
        context.user_data.pop("only_full_party", None)
        context.user_data.pop("own_position", None)
        clear_back(context)
        return ConversationHandler.END

    if data == "delta_custom":
        push_back(context, "SEARCH_MMR")
        text = "Введи значение Δ (положительное целое), например: 300\n(ММР будет искаться в диапазоне [your_mmr - Δ, your_mmr + Δ])"
        store_last_text(context, "SEARCH_MMR", text)
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
                                                                                InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]]))
        return SEARCH_MMR

    return ConversationHandler.END


# --- Text handlers ---


async def get_position(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return ConversationHandler.END
    txt = update.message.text.strip()
    if txt.lower() in ("отмена", "cancel"):
        prev = pop_back(context)
        await render_prev(prev, update, context)
        return ConversationHandler.END

    pos_key = txt
    if pos_key not in POSITIONS:
        await update.message.reply_text("❌ Введи цифру от 1 до 5!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
                                                                                                        InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]]))
        return POSITION
    position_name = POSITIONS[pos_key]
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    try:
        upsert_profile(user_id=user_id, position=position_name, username=username)
    except Exception as e:
        logger.exception("Ошибка БД при сохранении позиции: %s", e)
        await update.message.reply_text("Ошибка сохранения. Попробуй позже.", reply_markup=get_main_keyboard())
        return ConversationHandler.END
    await update.message.reply_text(f"✅ Позиция сохранена: {position_name}\n\nТеперь можешь искать тиммейта!", reply_markup=get_main_keyboard())
    return ConversationHandler.END


async def get_mmr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return ConversationHandler.END
    txt = update.message.text.strip()
    if txt.lower() in ("отмена", "cancel"):
        prev = pop_back(context)
        await render_prev(prev, update, context)
        return ConversationHandler.END
    try:
        mmr = int(txt)
        if mmr < 0 or mmr > 15000:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Введи корректное число MMR (от 0 до 15000)!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
                                                                                                                   InlineKeyboardButton("🏠 В меню", callback_data="main_menu"),
                                                                                                                   InlineKeyboardButton("Отмена", callback_data="go_back")]]))
        return MMR
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    try:
        upsert_profile(user_id=user_id, mmr=mmr, username=username)
    except Exception as e:
        logger.exception("Ошибка БД при сохранении MMR: %s", e)
        await update.message.reply_text("Ошибка сохранения. Попробуй позже.", reply_markup=get_main_keyboard())
        return ConversationHandler.END
    await update.message.reply_text("✅ MMR сохранён.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


async def get_search_mmr_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return ConversationHandler.END
    txt = update.message.text.strip()
    if txt.lower() in ("отмена", "cancel"):
        prev = pop_back(context)
        await render_prev(prev, update, context)
        return ConversationHandler.END
    try:
        delta = int(txt)
        if delta <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Введи корректное положительное число Δ (например: 300).", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="go_back"),
                                                                                                                           InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]]))
        return SEARCH_MMR

    user_id = update.message.from_user.id
    profile = get_profile(user_id)
    user_mmr = profile.get("mmr") if profile else None
    if user_mmr is None:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📝 Указать MMR", callback_data="edit_mmr")],
                                        [InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]])
        await update.message.reply_text("Чтобы фильтровать по MMR, сначала укажи свой MMR.", reply_markup=keyboard)
        return ConversationHandler.END

    search_mode = context.user_data.get("search_mode")
    exclude_pos = context.user_data.get("exclude_position", True)
    specific_pos = context.user_data.get("specific_position")
    only_full = context.user_data.get("only_full_party", False)

    logger.info("Starting search (custom Δ): user=%s mode=%s exclude_pos=%s specific_pos=%s only_full=%s mmr_filter=%s",
                user_id, search_mode, exclude_pos, specific_pos, only_full, delta)

    class DummyQuery:
        def __init__(self, update):
            self._update = update

        async def edit_message_text(self, text, reply_markup=None):
            await self._update.message.reply_text(text, reply_markup=reply_markup)

    dummy = DummyQuery(update)
    await perform_search_and_reply(dummy, user_id, search_mode, mmr_filter=delta, exclude_position=exclude_pos, specific_position=specific_pos, only_full_party=only_full)
    context.user_data.pop("search_mode", None)
    context.user_data.pop("exclude_position", None)
    context.user_data.pop("specific_position", None)
    context.user_data.pop("only_full_party", None)
    context.user_data.pop("own_position", None)
    clear_back(context)
    return ConversationHandler.END


# --- Search execution ---


async def perform_search_and_reply(
    query_obj,
    requester_id: int,
    search_mode: Optional[str],
    mmr_filter: Optional[int],
    exclude_position: Optional[bool] = None,
    specific_position: Optional[str] = None,
    only_full_party: Optional[bool] = None,
):
    """
    Выполняем поиск с учётом выбранных опций:
      - specific_position: если задано — ищем только её
      - иначе:
          - если exclude_position is True — исключаем позицию запроса
      - mode строгий: если указан search_mode, возвращаем только тех, у кого mode == search_mode
      - if only_full_party True -> full_party = 1
      - mmr filter applied if provided
      - only online users (online = 1)
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        requester_profile = get_profile(requester_id)
        requester_pos = requester_profile.get("position") if requester_profile else None
        requester_mmr = requester_profile.get("mmr") if requester_profile else None

        params = [requester_id]
        sql = "SELECT user_id, position, mode, mmr, username, full_party FROM profiles WHERE user_id != ? AND online = 1"

        # position filtering
        if specific_position:
            sql += " AND position = ?"
            params.append(specific_position)
        else:
            if exclude_position is True and requester_pos:
                sql += " AND (position IS NULL OR position != ?)"
                params.append(requester_pos)

        # mode strict
        if search_mode:
            sql += " AND LOWER(mode) = LOWER(?)"
            params.append(search_mode)

        # full party filter
        if only_full_party:
            sql += " AND full_party = 1"

        # mmr filter
        if mmr_filter is not None:
            if requester_mmr is None:
                await query_obj.edit_message_text("Чтобы фильтровать по MMR, у тебя должен быть указан MMR в профиле.", reply_markup=get_main_keyboard())
                conn.close()
                return
            min_m = max(0, requester_mmr - mmr_filter)
            max_m = requester_mmr + mmr_filter
            sql += " AND mmr BETWEEN ? AND ?"
            params.extend([min_m, max_m])

        sql += " LIMIT 30"

        logger.info("Выполняю SQL: %s | params=%s", sql, params)
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        logger.info("Найдено строк: %d", len(rows))

        conn.close()
    except Exception:
        logger.exception("Ошибка при выполнении поиска в БД")
        try:
            await query_obj.edit_message_text("Ошибка поиска. Подробности в логах.", reply_markup=get_main_keyboard())
        except Exception:
            logger.exception("Не удалось уведомить пользователя об ошибке поиска")
        return

    if not rows:
        try:
            await query_obj.edit_message_text("😔 Пока никто не найден по заданным критериям. Попробуй позже!", reply_markup=get_main_keyboard())
        except Exception:
            logger.exception("Не удалось отправить сообщение 'нет результатов'")
        return

    # Build results (use handshake emoji for full party, matching profile)
    combined_lines = []
    buttons = []
    for uid, pos, mode, user_mmr, username, full_party in rows:
        label = f"@{username}" if username else f"ID {uid}"
        fp_text = "🤝" if full_party else "—"
        combined_lines.append(f"👤 {label}\n🎯 {pos or '—'} | 🎮 {mode or '—'} | 📊 {user_mmr if user_mmr is not None else '—'} | {fp_text}")
        if username:
            buttons.append([InlineKeyboardButton(f"Написать {label}", url=f"https://t.me/{username}")])
        else:
            buttons.append([InlineKeyboardButton(f"Написать {label}", url=f"tg://user?id={uid}")])

    # add menu button
    buttons.append([InlineKeyboardButton("🏠 В меню", callback_data="main_menu")])

    combined_text = "Результаты поиска:\n\n" + "\n\n".join(combined_lines) + "\n\nНапиши игрокам, чтобы договориться о игре!"
    try:
        await query_obj.edit_message_text(text=combined_text, reply_markup=InlineKeyboardMarkup(buttons[:30]))
    except Exception:
        logger.exception("Не удалось отправить результаты поиска через edit_message_text")
        try:
            if hasattr(query_obj, "_update") and getattr(query_obj._update, "message", None):
                await query_obj._update.message.reply_text(text=combined_text, reply_markup=InlineKeyboardMarkup(buttons[:30]))
            elif getattr(query_obj, "message", None):
                await query_obj.message.reply_text(text=combined_text, reply_markup=InlineKeyboardMarkup(buttons[:30]))
        except Exception:
            logger.exception("Fallback отправки сообщения результатов не удался")


# --- Protected DB dump command (only for ADMIN_DUMP_USER_ID) ---


async def cmd_dump_profiles_protected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Вернёт содержимое таблицы profiles (в читаемом виде) ТОЛЬКО пользователю с ADMIN_DUMP_USER_ID.
    """
    if not update.message:
        return

    caller = update.message.from_user.id
    if caller != ADMIN_DUMP_USER_ID:
        await update.message.reply_text("У вас нет доступа к этой команде.")
        return

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, position, mode, mmr, username, online, full_party FROM profiles LIMIT 500")
        rows = cursor.fetchall()
        conn.close()
    except Exception:
        logger.exception("Ошибка при получении профилей для дампа")
        await update.message.reply_text("Ошибка при чтении БД. Проверьте логи.")
        return

    if not rows:
        await update.message.reply_text("База профилей пуста.")
        return

    lines = []
    for r in rows:
        uid, pos, mode, mmr, username, online, full_party = r
        lines.append(
            f"{uid} | pos={pos or '—'} | mode={mode or '—'} | mmr={mmr if mmr is not None else '—'} "
            f"| username={username or '—'} | online={'1' if online else '0'} | full={'1' if full_party else '0'}"
        )

    text = "\n".join(lines)
    chunk_size = 4000
    for i in range(0, len(text), chunk_size):
        await update.message.reply_text(text[i:i+chunk_size])


# --- Error handler ---


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(msg="Exception while handling an update:", exc_info=context.error)


# --- Main / setup ---


def main():
    # Замените на реальный токен бота
    token = "7523530357:AAGER5FNwAsVdOVNxPdhPuGUcZUEkjnmkhM"

    if token == "YOUR_BOT_TOKEN_HERE":
        logger.warning("BOT_TOKEN не установлен. Замените 'YOUR_BOT_TOKEN_HERE' на реальный токен.")

    application = Application.builder().token(token).build()

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler)],
        states={
            POSITION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_position)],
            MODE: [CallbackQueryHandler(button_handler)],
            MMR: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_mmr)],
            SEARCH_MODE: [CallbackQueryHandler(button_handler)],
            SEARCH_POS_OPTION: [CallbackQueryHandler(button_handler)],
            SELECT_POSITION: [CallbackQueryHandler(button_handler)],
            SEARCH_FULL_OPTION: [CallbackQueryHandler(button_handler)],
            SEARCH_MMR: [
                CallbackQueryHandler(button_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_search_mmr_custom),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)

    # Protected dump command (only for ADMIN_DUMP_USER_ID)
    application.add_handler(CommandHandler("dump_profiles", cmd_dump_profiles_protected))

    application.add_error_handler(error_handler)

    logger.info("Бот запущен. Ожидание обновлений...")
    application.run_polling()


if __name__ == "__main__":
    main()