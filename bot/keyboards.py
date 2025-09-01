"""Keyboard utilities for the Telegram bot."""

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup


def kb_quarters(curr: str, nxt: str) -> InlineKeyboardMarkup:
    """Create quarter selection keyboard."""
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Current {curr}", callback_data=f"q|{curr}")
    kb.button(text=f"Next {nxt}", callback_data=f"q|{nxt}")
    kb.adjust(2)
    return kb.as_markup()


def kb_opportunity_detail(opp_id: str, quarter: str) -> InlineKeyboardMarkup:
    """Create opportunity detail button."""
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Details", callback_data=f"opp|{opp_id}|{quarter}")
    return kb.as_markup()


def kb_scan_again(quarter: str) -> InlineKeyboardMarkup:
    """Create scan again button."""
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Scan Again", callback_data=f"scan|{quarter}")
    return kb.as_markup()