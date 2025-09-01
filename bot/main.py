"""AstroEdge Telegram Bot - Main handlers."""

import os
import asyncio
import logging
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

from keyboards import kb_quarters, kb_opportunity_detail, kb_scan_again
from formatting import current_quarter, next_quarter, fmt_pct, fmt_datetime, truncate_title
from api import (
    scan_quarter, get_opportunity_detail, get_aspects, health_check,
    get_positions, get_pnl, get_recent_fills, place_order,
    start_backtest, stop_backtest, get_backtest_status, list_backtests, get_kpis
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


@dp.message(Command("start"))
async def on_start(message: Message):
    """Handle /start command."""
    cq = current_quarter()
    nq = next_quarter()
    
    welcome_text = (
        "🌟 <b>Welcome to AstroEdge!</b>\n\n"
        "I help you find astrological opportunities in prediction markets.\n\n"
        "📊 Choose a quarter to scan for opportunities:"
    )
    
    await message.answer(welcome_text, reply_markup=kb_quarters(cq, nq))


@dp.message(Command("help"))
async def on_help(message: Message):
    """Handle /help command."""
    help_text = (
        "🤖 <b>AstroEdge Bot Commands</b>\n\n"
        "<b>📊 Analysis:</b>\n"
        "• /start - Main menu with quarter selection\n"
        "• /scan - Quick scan current quarter\n\n"
        "<b>💰 Trading (Paper):</b>\n"
        "• /positions - View current positions\n"
        "• /pnl - Show profit & loss\n"
        "• /fills - Recent trade fills\n\n"
        "<b>📈 Analytics & Backtesting:</b>\n"
        "• /kpis - Portfolio performance metrics\n"
        "• /backtest - Start a backtest run\n"
        "• /backtests - List recent backtests\n"
        "• /status - Get backtest status\n\n"
        "<b>ℹ️ Info:</b>\n"
        "• /help - Show this help message\n\n"
        "💡 <b>How it works:</b>\n"
        "1. Select a quarter to analyze opportunities\n"
        "2. View astrological market influences\n"
        "3. Paper trade based on insights\n"
        "4. Backtest strategies and analyze performance\n\n"
        "🔮 <i>Paper trading only - no real money at risk!</i>"
    )
    
    await message.answer(help_text)


@dp.message(Command("scan"))
async def on_scan(message: Message):
    """Handle /scan command for current quarter."""
    q = current_quarter()
    await message.answer(f"🔎 Scanning <b>{q}</b>… (this may take a few seconds)")
    
    try:
        # Check backend health first
        if not await health_check():
            await message.answer("❌ Backend service is currently unavailable. Please try again later.")
            return
            
        res = await scan_quarter(q)
        opportunities = res.get("opportunities", [])
        
        if not opportunities:
            await message.answer(
                f"📊 No opportunities found for <b>{q}</b>.\n\n"
                "Try another quarter or the scan parameters may need adjustment.",
                reply_markup=kb_scan_again(q)
            )
            return
        
        # Show top 10 opportunities
        top_opps = opportunities[:10]
        count_msg = f"🎯 Found <b>{len(opportunities)}</b> opportunities in <b>{q}</b>\n\n"
        
        if len(opportunities) > 10:
            count_msg += f"Showing top 10:\n\n"
            
        await message.answer(count_msg)
        
        for i, opp in enumerate(top_opps, 1):
            await send_opportunity_summary(message, opp, q, i)
            
    except Exception as e:
        logger.error(f"Error in scan command: {e}")
        await message.answer(
            f"❌ <b>Error scanning {q}</b>\n\n"
            f"Technical details: {str(e)[:100]}..."
        )


@dp.callback_query(F.data.startswith("q|"))
async def on_choose_quarter(callback: CallbackQuery):
    """Handle quarter selection."""
    _, q = callback.data.split("|", 1)
    
    await callback.message.answer(f"🔎 Scanning <b>{q}</b>… (this may take a few seconds)")
    
    try:
        # Check backend health first
        if not await health_check():
            await callback.message.answer("❌ Backend service is currently unavailable. Please try again later.")
            await callback.answer()
            return
            
        res = await scan_quarter(q)
        opportunities = res.get("opportunities", [])
        
        if not opportunities:
            await callback.message.answer(
                f"📊 No opportunities found for <b>{q}</b>.\n\n"
                "Try another quarter or the scan parameters may need adjustment.",
                reply_markup=kb_scan_again(q)
            )
            await callback.answer()
            return
        
        # Show summary and top opportunities
        summary = res.get("summary", {})
        total_scanned = summary.get("markets_scanned", "?")
        
        count_msg = (
            f"🎯 <b>Quarter {q} Results</b>\n\n"
            f"📊 Markets scanned: {total_scanned}\n"
            f"🔍 Opportunities found: <b>{len(opportunities)}</b>\n\n"
            f"Top 10 opportunities:"
        )
        
        await callback.message.answer(count_msg)
        
        # Show top 10 opportunities
        top_opps = opportunities[:10]
        for i, opp in enumerate(top_opps, 1):
            await send_opportunity_summary(callback.message, opp, q, i)
            
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error choosing quarter {q}: {e}")
        await callback.message.answer(
            f"❌ <b>Error scanning {q}</b>\n\n"
            f"Technical details: {str(e)[:100]}..."
        )
        await callback.answer()


@dp.callback_query(F.data.startswith("scan|"))
async def on_scan_again(callback: CallbackQuery):
    """Handle scan again button."""
    _, q = callback.data.split("|", 1)
    
    await callback.message.answer(f"🔄 Re-scanning <b>{q}</b>...")
    
    try:
        res = await scan_quarter(q)
        opportunities = res.get("opportunities", [])
        
        if not opportunities:
            await callback.message.answer(f"📊 Still no opportunities found for <b>{q}</b>.")
        else:
            await callback.message.answer(f"🎯 Found <b>{len(opportunities)}</b> opportunities!")
            for i, opp in enumerate(opportunities[:5], 1):  # Show top 5
                await send_opportunity_summary(callback.message, opp, q, i)
                
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error re-scanning {q}: {e}")
        await callback.message.answer(f"❌ Error re-scanning: {str(e)[:100]}...")
        await callback.answer()


@dp.callback_query(F.data.startswith("opp|"))
async def on_opportunity_detail(callback: CallbackQuery):
    """Handle opportunity detail request."""
    try:
        _, opp_id, quarter = callback.data.split("|", 2)
        
        await callback.answer("📊 Loading details...")
        
        data = await get_opportunity_detail(opp_id, quarter)
        opp = data["opportunity"]
        contributions = data["contributions"]
        
        market = opp.get("markets", {})
        title = truncate_title(market.get("title", "Unknown Market"), 50)
        
        # Main opportunity details
        detail_text = (
            f"📊 <b>{title}</b>\n\n"
            f"🎲 Base probability: {fmt_pct(opp['p0'])}\n"
            f"🌟 Astro probability: {fmt_pct(opp['p_astro'])}\n"
            f"💰 Net edge: <b>{fmt_pct(opp['edge_net'])}</b>\n"
            f"📈 Position size: {fmt_pct(opp['size_fraction'])}\n"
            f"🎯 Decision: <b>{opp['decision'].upper()}</b>\n\n"
        )
        
        # Add aspects contributing
        n_contribs = len(contributions)
        if n_contribs == 0:
            detail_text += "🔮 No aspects contributing."
        else:
            detail_text += f"🔮 <b>{n_contribs}</b> aspects contributing:\n\n"
            
            # Sort by absolute contribution and show top 5
            sorted_contribs = sorted(
                contributions, 
                key=lambda c: abs(c.get('contribution', 0)), 
                reverse=True
            )
            
            for contrib in sorted_contribs[:5]:
                aspect_event = contrib.get("aspect_events", {})
                contribution_val = contrib.get("contribution", 0)
                
                planet1 = aspect_event.get("planet1", "?")
                planet2 = aspect_event.get("planet2", "?")
                aspect = aspect_event.get("aspect", "?")
                peak_utc = aspect_event.get("peak_utc", "")
                is_eclipse = aspect_event.get("is_eclipse", False)
                
                eclipse_indicator = " 🌑" if is_eclipse else ""
                date_str = fmt_datetime(peak_utc)
                
                detail_text += (
                    f"• <b>{planet1}-{aspect}-{planet2}</b>{eclipse_indicator}\n"
                    f"  {date_str} → {contribution_val:+.2f}\n"
                )
                
            if len(sorted_contribs) > 5:
                detail_text += f"\n... and {len(sorted_contribs) - 5} more aspects"
        
        # Send the detailed message
        await callback.message.answer(detail_text)
        
    except Exception as e:
        logger.error(f"Error loading opportunity detail: {e}")
        await callback.message.answer(
            f"❌ <b>Error loading details</b>\n\n"
            f"Technical details: {str(e)[:100]}..."
        )
        await callback.answer()


async def send_opportunity_summary(message: Message, opp: dict, quarter: str, rank: int):
    """Send a summary of an opportunity."""
    title = truncate_title(opp.get("title", "Unknown Market"))
    
    # Format decision with emoji
    decision = opp.get("decision", "HOLD").upper()
    decision_emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(decision, "⚪")
    
    summary_text = (
        f"{rank}. <b>{title}</b>\n\n"
        f"🎲 {fmt_pct(opp['p0'])} → 🌟 {fmt_pct(opp['p_astro'])}\n"
        f"💰 Edge: <b>{fmt_pct(opp['edge_net'])}</b> | "
        f"📈 Size: {fmt_pct(opp['size_fraction'])}\n"
        f"{decision_emoji} <b>{decision}</b>"
    )
    
    await message.answer(
        summary_text, 
        reply_markup=kb_opportunity_detail(opp["id"], quarter)
    )


@dp.message(Command("positions"))
async def on_positions(message: Message):
    """Handle /positions command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
            
        positions = await get_positions()
        
        if not positions:
            await message.answer("📊 <b>No open positions</b>\n\nYour portfolio is currently empty.")
            return
        
        response = f"📊 <b>Current Positions ({len(positions)})</b>\n\n"
        
        total_unrealized = 0.0
        total_value = 0.0
        
        for pos in positions[:10]:  # Show top 10
            market_title = truncate_title(pos["market_title"], 40)
            qty = pos["qty"]
            vwap = pos["vwap"]
            mark = pos["mark_price"]
            unrealized = pos["unrealized_pnl"]
            current_val = pos["current_value"]
            
            total_unrealized += unrealized
            total_value += current_val
            
            pnl_emoji = "🟢" if unrealized > 0 else "🔴" if unrealized < 0 else "⚪"
            
            response += (
                f"{pnl_emoji} <b>{market_title}</b>\n"
                f"   {qty:.0f} shares @ ${vwap:.3f}\n"
                f"   Mark: ${mark:.3f} | P&L: ${unrealized:+.2f}\n\n"
            )
        
        if len(positions) > 10:
            response += f"... and {len(positions) - 10} more positions\n\n"
            
        response += (
            f"💰 <b>Total Unrealized:</b> ${total_unrealized:+.2f}\n"
            f"📈 <b>Total Value:</b> ${total_value:.2f}"
        )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in positions command: {e}")
        await message.answer(f"❌ <b>Error fetching positions</b>\n\n{str(e)[:100]}...")


@dp.message(Command("pnl"))
async def on_pnl(message: Message):
    """Handle /pnl command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
            
        pnl_data = await get_pnl()
        
        equity = pnl_data["equity_usdc"]
        realized = pnl_data["realized_usdc"]
        unrealized = pnl_data["unrealized_usdc"]
        fees = pnl_data["fees_usdc"]
        
        equity_emoji = "🟢" if equity > 0 else "🔴" if equity < 0 else "⚪"
        
        response = (
            f"💰 <b>Portfolio P&L</b>\n\n"
            f"{equity_emoji} <b>Total Equity:</b> ${equity:.2f}\n"
            f"💎 <b>Realized P&L:</b> ${realized:+.2f}\n"
            f"📊 <b>Unrealized P&L:</b> ${unrealized:+.2f}\n"
            f"💸 <b>Total Fees:</b> ${fees:.2f}\n\n"
            f"🕒 <i>Updated: {fmt_datetime(pnl_data['ts'])}</i>"
        )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in pnl command: {e}")
        await message.answer(f"❌ <b>Error fetching P&L</b>\n\n{str(e)[:100]}...")


@dp.message(Command("fills"))
async def on_fills(message: Message):
    """Handle /fills command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
            
        fills = await get_recent_fills(limit=10)
        
        if not fills:
            await message.answer("📋 <b>No recent fills</b>\n\nNo trades have been executed yet.")
            return
        
        response = f"📋 <b>Recent Fills ({len(fills)})</b>\n\n"
        
        for fill in fills:
            market_id = fill["market_id"]
            side = fill["side"]
            qty = fill["qty"]
            price = fill["price"]
            fee = fill["fee_usdc"]
            ts = fmt_datetime(fill["ts"])
            
            side_emoji = "🟢" if side == "YES" else "🔴"
            
            response += (
                f"{side_emoji} <b>{side}</b> {qty:.0f} @ ${price:.3f}\n"
                f"   {truncate_title(market_id, 35)}\n"
                f"   Fee: ${fee:.2f} | {ts}\n\n"
            )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in fills command: {e}")
        await message.answer(f"❌ <b>Error fetching fills</b>\n\n{str(e)[:100]}...")


@dp.message(Command("kpis"))
async def on_kpis(message: Message):
    """Handle /kpis command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
            
        kpis = await get_kpis()
        
        return_emoji = "🟢" if kpis["total_return"] > 0 else "🔴" if kpis["total_return"] < 0 else "⚪"
        
        response = (
            f"📈 <b>Portfolio Performance (KPIs)</b>\n\n"
            f"{return_emoji} <b>Total Return:</b> {kpis['total_return']*100:+.2f}%\n"
            f"📊 <b>Annualized Return:</b> {kpis['annualized_return']*100:+.2f}%\n"
            f"⚡ <b>Sharpe Ratio:</b> {kpis['sharpe_ratio']:.2f}\n"
            f"📉 <b>Max Drawdown:</b> {kpis['max_drawdown']*100:.1f}%\n"
            f"🎯 <b>Win Rate:</b> {kpis['win_rate']*100:.1f}%\n"
            f"💰 <b>Profit Factor:</b> {kpis['profit_factor']:.2f}\n\n"
            f"📈 <b>Trading Stats:</b>\n"
            f"• Total Trades: {kpis['total_trades']}\n"
            f"• Avg Trade P&L: ${kpis['avg_trade_pnl']:+.2f}\n"
            f"• Best Trade: ${kpis['best_trade']:+.2f}\n"
            f"• Worst Trade: ${kpis['worst_trade']:+.2f}\n"
            f"• Avg Hold Time: {kpis['avg_hold_time_hours']:.1f}h\n\n"
            f"💸 <b>Total Fees:</b> ${kpis['total_fees']:.2f}\n"
            f"📊 <b>Active Positions:</b> {kpis['current_positions']}\n"
            f"💵 <b>Total Volume:</b> ${kpis['total_volume']:.2f}"
        )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in kpis command: {e}")
        await message.answer(f"❌ <b>Error fetching KPIs</b>\n\n{str(e)[:100]}...")


@dp.message(Command("backtest"))
async def on_backtest(message: Message):
    """Handle /backtest command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
        
        # Simple backtest with default parameters
        config = {
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-12-31T23:59:59Z",
            "initial_capital": 1000.0,
            "scan_frequency": "daily",
            "lambda_gain": 0.10,
            "threshold": 0.04,
            "lambda_days": 5.0,
            "max_positions": 10,
            "max_position_size": 0.05,
            "fee_bps": 60
        }
        
        result = await start_backtest("Telegram Bot Backtest", config)
        
        response = (
            f"🚀 <b>Backtest Started!</b>\n\n"
            f"📊 <b>Run ID:</b> <code>{result['test_run_id']}</code>\n"
            f"📈 <b>Status:</b> {result['status']}\n\n"
            f"⚙️ <b>Parameters:</b>\n"
            f"• Period: 2024 (1 year)\n"
            f"• Initial Capital: $1,000\n"
            f"• Scan Frequency: Daily\n"
            f"• Max Positions: 10\n"
            f"• Max Position Size: 5%\n\n"
            f"💡 Use /status with the Run ID to check progress\n"
            f"💡 Use /backtests to see all your runs"
        )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in backtest command: {e}")
        await message.answer(f"❌ <b>Error starting backtest</b>\n\n{str(e)[:100]}...")


@dp.message(Command("backtests"))  
async def on_backtests(message: Message):
    """Handle /backtests command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
            
        backtests = await list_backtests(limit=10)
        
        if not backtests:
            await message.answer("📊 <b>No backtests found</b>\n\nUse /backtest to start your first backtest!")
            return
        
        response = f"📊 <b>Recent Backtests ({len(backtests)})</b>\n\n"
        
        for bt in backtests[:5]:  # Show top 5
            status_emoji = {
                "running": "🟡",
                "completed": "🟢", 
                "failed": "🔴",
                "stopped": "⚪"
            }.get(bt["status"], "❓")
            
            created_date = fmt_datetime(bt["created_at"])
            run_id_short = bt["id"][:8] + "..."
            
            response += (
                f"{status_emoji} <b>{truncate_title(bt['name'], 25)}</b>\n"
                f"   ID: <code>{run_id_short}</code> | {bt['status']} | {created_date}\n"
            )
            
            # Show metrics for completed backtests
            if bt["status"] == "completed" and bt.get("metrics"):
                metrics = bt["metrics"]
                total_return = metrics.get("total_return", 0) * 100
                sharpe = metrics.get("sharpe_ratio", 0)
                response += f"   📈 Return: {total_return:+.1f}% | Sharpe: {sharpe:.2f}\n"
            
            response += "\n"
        
        if len(backtests) > 5:
            response += f"... and {len(backtests) - 5} more\n\n"
            
        response += "💡 Use <code>/status &lt;run_id&gt;</code> for details"
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in backtests command: {e}")
        await message.answer(f"❌ <b>Error fetching backtests</b>\n\n{str(e)[:100]}...")


@dp.message(Command("status"))
async def on_status(message: Message):
    """Handle /status command."""
    try:
        if not await health_check():
            await message.answer("❌ Backend service unavailable")
            return
        
        # Extract test_run_id from message text
        text_parts = message.text.split()
        if len(text_parts) < 2:
            await message.answer(
                "❓ <b>Usage:</b> <code>/status &lt;run_id&gt;</code>\n\n"
                "Get the run ID from /backtests command"
            )
            return
        
        test_run_id = text_parts[1].strip()
        
        status = await get_backtest_status(test_run_id)
        
        status_emoji = {
            "running": "🟡",
            "completed": "🟢",
            "failed": "🔴", 
            "stopped": "⚪"
        }.get(status["status"], "❓")
        
        response = (
            f"{status_emoji} <b>Backtest Status</b>\n\n"
            f"📊 <b>Name:</b> {status['name']}\n"
            f"🆔 <b>ID:</b> <code>{status['id']}</code>\n"
            f"📈 <b>Status:</b> {status['status']}\n"
            f"📅 <b>Started:</b> {fmt_datetime(status['start_date'])}\n"
        )
        
        if status.get("end_date"):
            response += f"🏁 <b>Ended:</b> {fmt_datetime(status['end_date'])}\n"
        
        # Show metrics for completed backtests
        if status["status"] == "completed" and status.get("metrics"):
            metrics = status["metrics"]
            
            total_return = metrics.get("total_return", 0) * 100
            sharpe = metrics.get("sharpe_ratio", 0)
            max_dd = metrics.get("max_drawdown", 0) * 100
            win_rate = metrics.get("win_rate", 0) * 100
            total_trades = metrics.get("total_trades", 0)
            
            response += (
                f"\n📈 <b>Performance:</b>\n"
                f"• Total Return: {total_return:+.2f}%\n"
                f"• Sharpe Ratio: {sharpe:.2f}\n"
                f"• Max Drawdown: {max_dd:.1f}%\n"
                f"• Win Rate: {win_rate:.1f}%\n"
                f"• Total Trades: {total_trades}\n"
            )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in status command: {e}")
        await message.answer(f"❌ <b>Error getting backtest status</b>\n\n{str(e)[:100]}...")


async def main():
    """Main bot execution function."""
    logger.info("Starting AstroEdge Telegram Bot...")
    
    # Check backend connectivity
    if not await health_check():
        logger.warning("Backend health check failed - bot will continue but may have limited functionality")
    else:
        logger.info("Backend connectivity verified")
    
    # Start polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise