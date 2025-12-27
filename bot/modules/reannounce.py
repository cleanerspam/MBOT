from .. import task_dict, task_dict_lock, user_data, LOGGER
from ..core.config_manager import Config
from ..core.torrent_manager import TorrentManager
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.status_utils import get_task_by_gid
from ..helper.telegram_helper.bot_commands import BotCommands
from ..helper.telegram_helper.message_utils import send_message


@new_task
async def reannounce_torrent(_, message):
    """Manually trigger reannounce for a qBittorrent torrent."""
    user_id = (message.from_user or message.sender_chat).id
    msg = message.text.split()
    
    # Get task by GID or reply
    if len(msg) > 1:
        gid = msg[1]
        task = await get_task_by_gid(gid)
        if task is None:
            await send_message(message, f"GID: <code>{gid}</code> Not Found.")
            return
    elif reply_to_id := message.reply_to_message_id:
        async with task_dict_lock:
            task = task_dict.get(reply_to_id)
        if task is None:
            await send_message(message, "This is not an active task!")
            return
    else:
        msg = f"""Reply to an active task message or send GID to manually trigger reannounce.

<b>Usage:</b>
<code>/{BotCommands.ReannounceCommand[0]} GID</code>
or reply to task message:
<code>/{BotCommands.ReannounceCommand[0]}</code>

<b>Note:</b> This command only works for qBittorrent torrents.
Use this when torrent shows 0 seeders or is stuck."""
        await send_message(message, msg)
        return
    
    # Check permission
    if (
        Config.OWNER_ID != user_id
        and task.listener.user_id != user_id
        and (user_id not in user_data or not user_data[user_id].get("SUDO"))
    ):
        await send_message(message, "This task is not for you!")
        return
    
    # Check if it's a qBittorrent task
    if not task.listener.is_qbit:
        await send_message(message, "This command only works for qBittorrent torrents!")
        return
    
    try:
        # Get torrent hash
        if hasattr(task, 'hash'):
            torrent_hash = task.hash()
        elif hasattr(task, 'download_status') and hasattr(task.download_status, 'hash'):
            # For StreamingStatus
            torrent_hash = task.download_status.hash()
        else:
            await send_message(message, "Could not get torrent hash!")
            return
        
        # Get torrent info before reannounce
        try:
            tor_info = await TorrentManager.qbittorrent.torrents.info(hashes=[torrent_hash])
            if not tor_info:
                await send_message(message, "Torrent not found in qBittorrent!")
                return
            
            tor_info = tor_info[0]
            state = tor_info.state
            seeders_before = tor_info.num_seeds
            leechers = tor_info.num_leechs
            
            LOGGER.info(f"[REANNOUNCE] Manual reannounce requested for {tor_info.name}")
            LOGGER.info(f"[REANNOUNCE] State: {state}, Seeders: {seeders_before}, Leechers: {leechers}")
        except Exception as e:
            LOGGER.error(f"[REANNOUNCE] Failed to get torrent info: {e}")
            await send_message(message, f"Failed to get torrent info: {e}")
            return
        
        # Trigger reannounce
        try:
            await TorrentManager.qbittorrent.torrents.reannounce([torrent_hash])
            LOGGER.info(f"[REANNOUNCE] Reannounce triggered for {torrent_hash}")
            
            # Wait a bit for trackers to respond
            import asyncio
            await asyncio.sleep(3)
            
            # Get updated info
            tor_info_after = await TorrentManager.qbittorrent.torrents.info(hashes=[torrent_hash])
            if tor_info_after:
                tor_info_after = tor_info_after[0]
                seeders_after = tor_info_after.num_seeds
                
                response = f"""<b>Reannounce Triggered!</b>

<b>Torrent:</b> <code>{tor_info.name}</code>
<b>State:</b> {state}

<b>Before Reannounce:</b>
├ Seeders: {seeders_before}
└ Leechers: {leechers}

<b>After Reannounce (3s later):</b>
├ Seeders: {seeders_after}
└ Leechers: {tor_info_after.num_leechs}"""
                
                if seeders_after > seeders_before:
                    response += f"\n\n✅ <b>Success!</b> Seeders increased by {seeders_after - seeders_before}"
                elif seeders_after == seeders_before and seeders_before == 0:
                    response += "\n\n⚠️ <b>Note:</b> Still 0 seeders. This could be:\n• Tracker offline\n• No seeders in swarm\n• DHT/PEX needed time to discover peers"
                else:
                    response += "\n\n✅ Reannounce completed"
                
                await send_message(message, response)
            else:
                await send_message(message, "Reannounce triggered successfully! Check /status for updated seeder count.")
                
        except Exception as e:
            LOGGER.error(f"[REANNOUNCE] Failed to reannounce: {e}")
            await send_message(message, f"Failed to trigger reannounce: {e}")
            
    except Exception as e:
        LOGGER.error(f"[REANNOUNCE] Error: {e}", exc_info=True)
        await send_message(message, f"Error: {e}")
