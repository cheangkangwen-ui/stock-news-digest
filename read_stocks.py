import os
import sys
import asyncio
import anthropic

# Fix Unicode printing on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.functions.channels import CreateChannelRequest
from datetime import datetime, timezone, timedelta

TELEGRAM_API_ID = 33919151
TELEGRAM_API_HASH = "dd0a935bd6545cf56910292ff4445c4e"
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "my_session")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
STOCK_GROUP_NAME = os.environ.get("STOCK_GROUP_NAME", "📈 Stock Digest")

TG_SEMAPHORE = 30

SEARCH_TOOL = {
    "name": "web_search",
    "description": (
        "Search the web for current news, company info, or financial context. "
        "Use this when you need to look up a company, ticker, or topic that is unclear from the messages alone."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "The search query"}
        },
        "required": ["query"],
    },
}


def web_search(query: str, max_results: int = 6) -> str:
    from duckduckgo_search import DDGS
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        if not results:
            return "No results found."
        return "\n\n".join(
            f"**{r['title']}**\n{r['href']}\n{r['body']}" for r in results
        )
    except Exception as e:
        return f"Search failed: {e}"


def get_time_window():
    """Always look back 24 hours."""
    myt = timezone(timedelta(hours=8))
    now = datetime.now(myt)
    start = now - timedelta(hours=24)
    label = f"Last 24h ({start.strftime('%Y-%m-%d %H:%M')} - {now.strftime('%H:%M')} MYT)"
    return start.astimezone(timezone.utc), label


async def get_or_create_stock_group(tg):
    dialogs = await tg.get_dialogs()
    for d in dialogs:
        if d.name == STOCK_GROUP_NAME and getattr(d.entity, "megagroup", False):
            return d.entity
    result = await tg(CreateChannelRequest(
        title=STOCK_GROUP_NAME,
        about="Automated stock news digests",
        megagroup=True,
    ))
    return result.chats[0]


async def fetch_channel(tg, dialog, start_utc, now_utc, sem):
    async with sem:
        messages = []
        try:
            async for m in tg.iter_messages(dialog, offset_date=now_utc):
                if not m.date:
                    continue
                if m.date < start_utc:
                    break
                if m.text:
                    messages.append(m)
        except Exception:
            pass
        return messages


async def main():
    ai_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    session = StringSession(TELEGRAM_SESSION) if len(TELEGRAM_SESSION) > 20 else TELEGRAM_SESSION
    tg = TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await tg.connect()

    if not await tg.is_user_authorized():
        await tg.disconnect()
        raise Exception("Not authorized.")

    try:
        stock_group = await get_or_create_stock_group(tg)

        # Duplicate guard: skip if a STOCK DIGEST was sent within the last 10 minutes
        if not os.environ.get("SKIP_DUPLICATE_CHECK"):
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
            async for msg in tg.iter_messages(stock_group, limit=3):
                if msg.date and msg.date >= cutoff and msg.text and "STOCK DIGEST" in msg.text:
                    print("Stock digest already sent in last 10 minutes. Skipping.")
                    return

        # Fixed 24-hour lookback
        now_utc = datetime.now(timezone.utc)
        start_utc, label = get_time_window()

        print(f"\n{'='*70}")
        print(f"  STOCK NEWS ANALYSIS  |  Window: {label}")
        print(f"{'='*70}\n")

        # Get channels from "Stock News" folder only
        filters_result = await tg(GetDialogFiltersRequest())
        filter_list = filters_result.filters if hasattr(filters_result, 'filters') else filters_result

        def folder_title(f):
            t = getattr(f, 'title', None)
            if t is None:
                return None
            return t.text if hasattr(t, 'text') else str(t)

        stock_folder = next(
            (f for f in filter_list if folder_title(f) == "Stock News"), None
        )
        if not stock_folder:
            print("ERROR: 'Stock News' folder not found in Telegram.")
            return

        folder_peer_ids = {
            p.channel_id for p in stock_folder.include_peers if hasattr(p, 'channel_id')
        }
        print(f"  'Stock News' folder contains {len(folder_peer_ids)} channel(s).")

        dialogs = await tg.get_dialogs()
        channels = [
            d for d in dialogs
            if isinstance(d.entity, Channel) and d.entity.id in folder_peer_ids
        ]

        active_channels = [
            d for d in channels
            if d.message and d.message.date and d.message.date >= start_utc
        ]
        print(f"  {len(channels)} channels in folder, {len(active_channels)} posted in window. Fetching...\n")

        tg_sem = asyncio.Semaphore(TG_SEMAPHORE)
        tasks = [fetch_channel(tg, d, start_utc, now_utc, tg_sem) for d in active_channels]
        results = await asyncio.gather(*tasks)

        all_messages = []
        for dialog, messages in zip(active_channels, results):
            if not messages:
                continue
            channel_block = f"### {dialog.name}\n" + "\n".join(
                f"[{m.date.astimezone().strftime('%H:%M')}] {m.text[:400]}"
                for m in reversed(messages)
            )
            all_messages.append(channel_block)

        print(f"  Got messages from {len(all_messages)} channels.")

        if not all_messages:
            print("  No messages found in window.")
            return

        raw_dump = "\n\n".join(all_messages)

        prompt = f"""You are a financial educator explaining stock news to a layman investor. Below are raw messages from {len(all_messages)} Telegram channels in a "Stock News" folder, covering {label}.

For each significant story, use the web_search tool to look up the company or context if you are unsure about the ticker, what the company does, or why the news matters. Only search where context would meaningfully improve the explanation.

IMPORTANT: Keep the entire digest CONCISE — aim for under 3500 characters total (excluding sources). Be dense and informative, not verbose.

Produce a digest in this exact format:

📈 STOCK DIGEST | {label}

Group stories by sector. For each stock/story, use this COMPACT format:
<ticker> — <one-line news headline>
↳ Why it matters: <1-2 sentences, plain English, no jargon without explanation>
↳ Sentiment: Bullish/Bearish/Neutral

Sectors (skip any with no stories): AI/Semis/Tech | Energy/Commodities | Financials | Healthcare | Other/Macro

SUMMARY: 2-3 sentences on key themes across all sectors, written for a layman.

Rules:
- Merge duplicate stories across channels into one entry
- Skip channels with no stock-relevant content
- AI/Semiconductors/Tech always comes first
- Omit sections with no relevant stories — do not write "[No significant stories]"
- Maximum 10-12 stock entries total — prioritise the most impactful

After the digest, output this EXACT line on its own:
---SOURCES---

Then list source references as numbered items. For each stock/claim, cite the Telegram channel name it came from. For web searches, include the URL. Format:
1. [Ticker or topic] — Channel Name or URL
2. ...

RAW MESSAGES:
{raw_dump}"""

        print("  Sending to Opus for analysis...\n")

        loop = asyncio.get_event_loop()

        def _call():
            messages = [{"role": "user", "content": prompt}]
            while True:
                response = ai_client.messages.create(
                    model="claude-opus-4-6",
                    max_tokens=4000,
                    thinking={"type": "adaptive"},
                    tools=[SEARCH_TOOL],
                    messages=messages,
                )
                if response.stop_reason != "tool_use":
                    return response
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        print(f"  [web search] {block.input['query']}")
                        result = web_search(block.input["query"])
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

        response = await loop.run_in_executor(None, _call)

        digest_text = ""
        for block in response.content:
            if block.type == "text":
                digest_text = block.text.strip()
                for line in digest_text.split("\n"):
                    print(f"  {line}")

        if digest_text:
            print("\n  Sending to Telegram...")

            # Split digest from sources
            if "---SOURCES---" in digest_text:
                body, sources = digest_text.split("---SOURCES---", 1)
                body = body.strip()
                sources = sources.strip()
            else:
                body = digest_text
                sources = ""

            full_text = body
            chunk_size = 4000
            chunks = []
            while len(full_text) > chunk_size:
                split_at = full_text.rfind("\n", 0, chunk_size)
                if split_at == -1:
                    split_at = chunk_size
                chunks.append(full_text[:split_at])
                full_text = full_text[split_at:].lstrip("\n")
            if full_text:
                chunks.append(full_text)
            first_msg = None
            for i, chunk in enumerate(chunks):
                if len(chunks) > 1:
                    chunk = f"[{i+1}/{len(chunks)}]\n\n" + chunk
                sent = await tg.send_message(stock_group, chunk)
                if i == 0:
                    first_msg = sent
                await asyncio.sleep(0.5)

            # Send sources as a separate message
            if sources:
                sources_msg = f"🔗 SOURCES\n{'='*40}\n\n{sources}"
                await asyncio.sleep(0.5)
                await tg.send_message(stock_group, sources_msg)

            if first_msg:
                await tg.pin_message(stock_group, first_msg.id, notify=False)
            print(f"  Sent {len(chunks)} digest message(s)" + (" + 1 sources message." if sources else "."))

        print(f"\n{'='*70}")
        print("  Done.")
        print(f"{'='*70}\n")

    finally:
        await tg.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
