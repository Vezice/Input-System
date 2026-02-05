"""
iBot v2 Slack Notifications

Sends notifications to Slack for import status updates.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from config import settings
from parser import ParsedFile
from utils.logger import get_logger

logger = get_logger(__name__)


class SlackNotifier:
    """Handles Slack notifications for iBot imports."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        mention_user: Optional[str] = None,
    ):
        self.webhook_url = webhook_url or settings.SLACK_WEBHOOK_URL
        self.mention_user = mention_user or settings.SLACK_MENTION_USER
        self.enabled = settings.SLACK_ENABLED and bool(self.webhook_url)

        if not self.enabled:
            logger.info("Slack notifications disabled")

    async def send_message(
        self,
        text: str,
        blocks: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        """Send a message to Slack."""
        if not self.enabled:
            logger.debug(f"Slack disabled, would send: {text}")
            return False

        payload = {"text": text}
        if blocks:
            payload["blocks"] = blocks

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=10.0,
                )

            if response.status_code == 200:
                logger.debug("Slack message sent successfully")
                return True
            else:
                logger.warning(f"Slack API error: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Failed to send Slack message: {e}")
            return False

    def _create_blocks(
        self,
        title: str,
        fields: List[Dict[str, str]],
        footer: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Create rich message blocks."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": title,
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{f['label']}*\n{f['value']}"
                    }
                    for f in fields
                ]
            },
        ]

        if footer:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": footer}]
            })

        return blocks

    async def notify_success(
        self,
        parsed_file: ParsedFile,
        brand_code: str,
        import_id: str,
        rows_inserted: int,
        duration_seconds: float,
    ) -> bool:
        """Send success notification."""
        category_name = parsed_file.category.name if parsed_file.category else "Unknown"

        text = (
            f":white_check_mark: *Import Berhasil*\n"
            f"File `{parsed_file.filename}` telah diimport ke `{category_name}`"
        )

        fields = [
            {"label": "File", "value": parsed_file.filename},
            {"label": "Brand", "value": brand_code},
            {"label": "Kategori", "value": category_name},
            {"label": "Rows", "value": f"{rows_inserted:,}"},
            {"label": "Durasi", "value": f"{duration_seconds:.1f}s"},
        ]

        blocks = self._create_blocks(
            title=":white_check_mark: Import Berhasil",
            fields=fields,
            footer=f"Import ID: {import_id} | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        )

        return await self.send_message(text, blocks)

    async def notify_failure(
        self,
        filename: str,
        brand_code: str,
        category_name: str,
        error: str,
    ) -> bool:
        """Send failure notification."""
        mention = f" {self.mention_user}" if self.mention_user else ""

        text = (
            f":x: *Import Gagal*{mention}\n"
            f"File `{filename}` gagal diimport.\n"
            f"Error: {error}"
        )

        fields = [
            {"label": "File", "value": filename},
            {"label": "Brand", "value": brand_code},
            {"label": "Kategori", "value": category_name},
            {"label": "Error", "value": error[:200]},
        ]

        blocks = self._create_blocks(
            title=":x: Import Gagal",
            fields=fields,
            footer=f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        )

        return await self.send_message(text, blocks)


# Module-level cache for notifiers
_notifiers: Dict[str, SlackNotifier] = {}


def get_notifier(webhook_url: Optional[str] = None) -> SlackNotifier:
    """Get or create a Slack notifier instance."""
    cache_key = webhook_url or "default"

    if cache_key not in _notifiers:
        _notifiers[cache_key] = SlackNotifier(webhook_url=webhook_url)

    return _notifiers[cache_key]
