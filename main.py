from __future__ import annotations

import asyncio
import html as html_lib
import inspect
import json
import os
import platform
import re
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import requests
import nio

from compact_encryption import CompactEncryptor


@dataclass(slots=True)
class EventData:
    name: str = ""
    timestamp: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Profile:
    time_window_sec: int = 60
    beschreibung_format: str = ""
    output_format: str = ""
    sender: str | None = None
    log_file: str | None = None

    rc_host: str | None = None
    rc_port: str | None = None
    rc_user: str | None = None
    rc_passwd: str | None = None
    rc_channel: str | None = None

    tg_bot_token: str | None = None
    tg_chat_id: str | None = None
    tg_parse_mode: str | None = None

    pushover_app_token: str | None = None
    pushover_user_key: str | None = None

    discord_wh_url: str | None = None

    mtx_room: str | None = None


@dataclass(slots=True)
class Config:
    tcp_host: str = "0.0.0.0"
    tcp_port: int = 9000
    mtx_homeserver: str = "https://matrix.org"
    mtx_user: str = "user:matrix.org"
    mtx_passwd: str = "EinPasswort"
    mtx_access_token: str = ""
    mtx_deviceId: str = "Mergerbot".upper()
    mtx_store_path: str = "./data/"
    mtx_session_file: str = "./data/matrix_session.json"
    mtx_ignore_unverified_devices: bool = True
    mtx_require_encryption: bool = True
    default: Profile = field(default_factory=Profile)
    profiles: dict[str, Profile] = field(default_factory=dict)


CONFIG: Config | None = None
MTX_CLIENT: nio.AsyncClient | None = None
MATRIX_SENDER: "MatrixSender | None" = None
PENDING: dict[str, asyncio.Queue[EventData]] = {}
WORKER_TASKS: dict[str, asyncio.Task[None]] = {}
RIC_ENCRYPTOR = CompactEncryptor(secret_key=984264)


def _profile_from_dict(data: dict[str, Any]) -> Profile:
    return Profile(
        time_window_sec=int(data.get("time_window_sec", 60)),
        beschreibung_format=str(data.get("beschreibung_format", "")),
        output_format=str(data.get("output_format", "")),
        sender=data.get("sender"),
        log_file=data.get("log_file"),
        rc_host=data.get("rc_host"),
        rc_port=data.get("rc_port"),
        rc_user=data.get("rc_user"),
        rc_passwd=data.get("rc_passwd"),
        rc_channel=data.get("rc_channel"),
        tg_bot_token=data.get("tg_bot_token"),
        tg_chat_id=data.get("tg_chat_id"),
        tg_parse_mode=data.get("tg_parse_mode"),
        pushover_app_token=data.get("pushover_app_token"),
        pushover_user_key=data.get("pushover_user_key"),
        discord_wh_url=data.get("discord_wh_url"),
        mtx_room=data.get("mtx_room")
    )


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_config(path: Path) -> Config:
    raw = json.loads(path.read_text(encoding="utf-8"))
    default_profile = _profile_from_dict(raw.get("default", {}))
    profiles = {
        name: _profile_from_dict(profile_data)
        for name, profile_data in raw.get("profiles", {}).items()
    }
    return Config(
        tcp_host=str(raw.get("tcp_host", "0.0.0.0")),
        tcp_port=int(raw.get("tcp_port", 9000)),
        mtx_homeserver=str(raw.get("mtx_homeserver", "")),
        mtx_user=str(raw.get("mtx_user", raw.get("mtx_user_id", ""))),
        mtx_passwd=str(raw.get("mtx_passwd", raw.get("mtx_password", ""))),
        mtx_access_token=str(raw.get("mtx_access_token", "")),
        mtx_deviceId=str(raw.get("mtx_deviceId", raw.get("mtx_device_id", "MERGERMAIN"))),
        mtx_store_path=str(raw.get("mtx_store_path", "./data/")),
        mtx_session_file=str(raw.get("mtx_session_file", "./data/matrix_session.json")),
        mtx_ignore_unverified_devices=_as_bool(raw.get("mtx_ignore_unverified_devices"), True),
        mtx_require_encryption=_as_bool(raw.get("mtx_require_encryption"), True),
        default=default_profile,
        profiles=profiles,
    )


def get_profile(name: str) -> Profile:
    assert CONFIG is not None
    return CONFIG.profiles.get(name, CONFIG.default)


def escape_discord_markdown(text: str | None) -> str | None:
    if text is None:
        return None
    for c in ["\\", "*", "_", "~", "`", "|", ">", "#"]:
        text = text.replace(c, "\\" + c)
    return text


def xml_to_dict(element: ET.Element) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for child in list(element):
        if list(child):
            out[child.tag] = xml_to_dict(child)
        else:
            out[child.tag] = child.text or ""
    return out


def parse_xml(xml_data: str) -> EventData:
    root = ET.fromstring(xml_data)
    data = xml_to_dict(root)
    user = str(data.get("user", "Unknown"))
    return EventData(name=user, timestamp=int(time.time()), raw=data)


def _resolve_path(raw: dict[str, Any], path: str) -> str:
    value: Any = raw
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return ""
    if isinstance(value, dict):
        return ""
    return "" if value is None else str(value)


def replace_placeholders(template: str, event_data: EventData) -> str:
    text = template
    dt = datetime.fromtimestamp(event_data.timestamp)
    text = text.replace("%timestamp%", dt.strftime("%d.%m.%Y %H:%M:%S"))

    def replace_enc_ric(_: re.Match[str]) -> str:
        try:
            telegram = event_data.raw.get("telegram")
            if isinstance(telegram, dict):
                address_obj = telegram.get("address")
                if address_obj is not None:
                    return RIC_ENCRYPTOR.encrypt(int(str(address_obj)))
        except Exception as exc:
            print(f"Fehler beim Verschluesseln von address mit enc_ric: {exc}")
        return ""

    text = re.sub(r"%enc_ric%", replace_enc_ric, text)

    def replace_generic(match: re.Match[str]) -> str:
        return _resolve_path(event_data.raw, match.group(1))

    return re.sub(r"%([\w\.]+)%", replace_generic, text)


def format_output(events: list[EventData], profile: Profile) -> str:
    beschreibungen = [replace_placeholders(profile.beschreibung_format, e) for e in events]
    output = profile.output_format.replace("%beschreibungen%", "\n".join(beschreibungen))
    return replace_placeholders(output, events[0])


async def write_log(profile: Profile, text: str) -> None:
    log_file = profile.log_file or "logs/default.log"
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(text + "\n" + "-" * 40 + "\n")


async def send_to_rocketchat(profile: Profile, text: str) -> None:
    rc_channel = profile.rc_channel or ""
    print(f"RocketChat-Nachricht wuerde an {rc_channel} gesendet")


async def send_to_telegram(profile: Profile, text: str) -> None:
    bot_token = profile.tg_bot_token or ""
    chat_id = profile.tg_chat_id or "0"
    parse_mode = "HTML" if (profile.tg_parse_mode or "").lower() == "html" else "MarkdownV2"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    resp = await asyncio.to_thread(requests.post, url, json=payload, timeout=15)
    print(f"Telegram-Nachricht gesendet: {resp.status_code}")


async def send_to_pushover(profile: Profile, text: str) -> None:
    payload = {
        "token": profile.pushover_app_token or "",
        "user": profile.pushover_user_key or "",
        "message": text,
    }
    resp = await asyncio.to_thread(
        requests.post,
        "https://api.pushover.net/1/messages.json",
        data=payload,
        timeout=15,
    )
    print(f"Pushover Nachricht gesendet: {resp.status_code}")


async def send_to_discord_webhook(profile: Profile, text: str) -> None:
    webhook_url = profile.discord_wh_url or ""
    resp = await asyncio.to_thread(requests.post, webhook_url, json={"content": text}, timeout=15)
    print(f"Discord Webhook Nachricht gesendet: {resp.status_code}")


def _html_to_matrix_plaintext(text: str) -> str:
    plain = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    plain = re.sub(r"<\s*/\s*p\s*>", "\n\n", plain, flags=re.IGNORECASE)
    plain = re.sub(r"<[^>]+>", "", plain)
    plain = html_lib.unescape(plain)
    plain = plain.replace("\r\n", "\n").replace("\r", "\n")
    plain = re.sub(r"\n{3,}", "\n\n", plain).strip()
    return plain or "-"


def _normalize_matrix_html(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.strip()
    return normalized.replace("\n", "<br/>")


class MatrixSender:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._client: nio.AsyncClient | None = None
        self._resolved_user = self._normalize_user_id(config.mtx_user, config.mtx_homeserver)
        self._sync_task: asyncio.Task[None] | None = None

    @property
    def client(self) -> nio.AsyncClient:
        if self._client is None:
            raise RuntimeError("Matrix client ist nicht initialisiert")
        return self._client

    @property
    def _session_path(self) -> Path:
        return Path(self._config.mtx_session_file)

    def _build_client(self) -> nio.AsyncClient:
        Path(self._config.mtx_store_path).mkdir(parents=True, exist_ok=True)
        client_config = nio.AsyncClientConfig(encryption_enabled=True)
        return nio.AsyncClient(
            homeserver=self._config.mtx_homeserver,
            user=self._resolved_user,
            store_path=self._config.mtx_store_path,
            config=client_config,
        )

    def _normalize_user_id(self, value: str, homeserver: str) -> str:
        user = value.strip()
        if not user:
            return ""
        if user.startswith("@") and ":" in user:
            return user
        server = homeserver.replace("https://", "").replace("http://", "").strip("/")
        if not server:
            return user
        if user.startswith("@") and ":" not in user:
            return f"{user}:{server}"
        if ":" in user:
            return user if user.startswith("@") else f"@{user}"
        return f"@{user}:{server}"

    def _load_session(self) -> dict[str, str] | None:
        try:
            raw = json.loads(self._session_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception as exc:
            print(f"Matrix Session-Datei ungueltig, ignoriere sie: {exc}")
            return None

        required = ("user_id", "device_id", "access_token")
        if not all(raw.get(key) for key in required):
            return None
        return {key: str(raw[key]) for key in required}

    def _save_session(self, user_id: str, device_id: str, access_token: str) -> None:
        self._session_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "user_id": user_id,
            "device_id": device_id,
            "access_token": access_token,
        }
        self._session_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    async def _whoami_from_token(self, access_token: str) -> dict[str, str]:
        homeserver = self._config.mtx_homeserver.rstrip("/")
        url = f"{homeserver}/_matrix/client/v3/account/whoami"
        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            response = await asyncio.to_thread(requests.get, url, headers=headers, timeout=15)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"Matrix whoami fehlgeschlagen: {exc}") from exc

        user_id = str(payload.get("user_id", "")).strip()
        device_id = str(payload.get("device_id", "")).strip()
        if not user_id:
            raise RuntimeError("Matrix whoami lieferte keine user_id fuer das Access-Token")

        if not device_id:
            device_id = self._config.mtx_deviceId.strip() or "MERGERMAIN"

        return {
            "user_id": user_id,
            "device_id": device_id,
            "access_token": access_token,
        }

    async def connect(self) -> None:
        session = self._load_session()
        use_explicit_token = bool(self._config.mtx_access_token)
        if use_explicit_token and session is not None and session["access_token"] != self._config.mtx_access_token:
            session = None

        token_session: dict[str, str] | None = None
        if session is not None:
            self._resolved_user = session["user_id"]
        elif use_explicit_token:
            token_session = await self._whoami_from_token(self._config.mtx_access_token)
            self._resolved_user = token_session["user_id"]
        elif not self._resolved_user:
            raise RuntimeError("Fuer Matrix Passwort-Login ist mtx_user erforderlich")

        self._client = self._build_client()
        client = self.client

        if session is not None:
            client.restore_login(
                user_id=session["user_id"],
                device_id=session["device_id"],
                access_token=session["access_token"],
            )
            print(f"Matrix Session wiederhergestellt mit Device {session['device_id']}")
        elif use_explicit_token:
            assert token_session is not None
            client.restore_login(
                user_id=token_session["user_id"],
                device_id=token_session["device_id"],
                access_token=token_session["access_token"],
            )
            self._save_session(
                token_session["user_id"],
                token_session["device_id"],
                token_session["access_token"],
            )
            print(f"Matrix Token-Login aktiv mit Device {token_session['device_id']}")
        else:
            if not self._config.mtx_passwd:
                raise RuntimeError("Matrix Passwort fehlt (mtx_passwd) und kein mtx_access_token gesetzt")

            login_kwargs: dict[str, Any] = {"device_name": self._config.mtx_deviceId}
            login_sig = inspect.signature(client.login)
            if "device_id" in login_sig.parameters:
                login_kwargs["device_id"] = self._config.mtx_deviceId

            login_resp = await client.login(self._config.mtx_passwd, **login_kwargs)
            if isinstance(login_resp, nio.LoginError):
                raise RuntimeError(f"Matrix-Login fehlgeschlagen: {login_resp.message}")

            user_id = str(getattr(login_resp, "user_id", "")) or self._resolved_user
            device_id = str(getattr(login_resp, "device_id", "")) or self._config.mtx_deviceId
            access_token = str(getattr(login_resp, "access_token", ""))
            if access_token:
                self._save_session(user_id, device_id, access_token)
            print(f"Matrix Passwort-Login erfolgreich mit Device {device_id}")

        if client.should_upload_keys:
            await client.keys_upload()

        # Initialer Sync ist fuer Raumzustand und Schluesselaustausch notwendig.
        await client.sync(timeout=30000, full_state=True)
        await self._accept_all_invites()
        self._sync_task = asyncio.create_task(self._sync_loop())

    async def close(self) -> None:
        if self._sync_task is not None:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def _accept_all_invites(self) -> None:
        client = self.client
        invited_room_ids = list(client.invited_rooms.keys())
        for room_id in invited_room_ids:
            join_resp = await client.join(room_id)
            if isinstance(join_resp, nio.JoinError):
                print(f"Matrix Einladung konnte nicht angenommen werden ({room_id}): {join_resp.message}")
            else:
                print(f"Matrix Einladung angenommen: {room_id}")

    async def _sync_loop(self) -> None:
        client = self.client
        while True:
            try:
                await client.sync(timeout=30000, full_state=False)
                await self._accept_all_invites()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Matrix Sync-Loop Fehler: {exc}")
                await asyncio.sleep(2)

    async def _ensure_joined(self, room_id: str) -> None:
        client = self.client
        if room_id in client.rooms:
            return
        join_resp = await client.join(room_id)
        if isinstance(join_resp, nio.JoinError):
            raise RuntimeError(f"Matrix join fehlgeschlagen: {join_resp.message}")
        await client.sync(timeout=10000, full_state=True)

    async def _ensure_encryption(self, room_id: str) -> None:
        if not self._config.mtx_require_encryption:
            return

        client = self.client
        room = client.rooms.get(room_id)
        if room is not None and bool(getattr(room, "encrypted", False)):
            return

        if not hasattr(client, "room_put_state"):
            raise RuntimeError("Kann Raumverschluesselung nicht setzen (room_put_state nicht verfuegbar)")

        state_resp = await client.room_put_state(
            room_id=room_id,
            event_type="m.room.encryption",
            content={"algorithm": "m.megolm.v1.aes-sha2"},
        )
        if isinstance(state_resp, nio.RoomPutStateError):
            raise RuntimeError(f"Raumverschluesselung konnte nicht aktiviert werden: {state_resp.message}")

        await client.sync(timeout=10000, full_state=True)
        room = client.rooms.get(room_id)
        if room is None or not bool(getattr(room, "encrypted", False)):
            raise RuntimeError("Raum ist nicht verschluesselt. Bitte im Matrix-Client Verschluesselung aktivieren.")

    async def send_html(self, room_id: str, html_text: str) -> None:
        await self._ensure_joined(room_id)
        await self._ensure_encryption(room_id)

        client = self.client
        formatted_html = _normalize_matrix_html(html_text)
        content = {
            "msgtype": "m.notice",
            "body": _html_to_matrix_plaintext(html_text),
            "format": "org.matrix.custom.html",
            "formatted_body": formatted_html,
        }

        send_kwargs: dict[str, Any] = {
            "room_id": room_id,
            "message_type": "m.room.message",
            "content": content,
        }
        send_sig = inspect.signature(client.room_send)
        if "ignore_unverified_devices" in send_sig.parameters:
            send_kwargs["ignore_unverified_devices"] = self._config.mtx_ignore_unverified_devices

        send_resp = await client.room_send(**send_kwargs)
        if isinstance(send_resp, nio.RoomSendError):
            raise RuntimeError(f"Matrix send fehlgeschlagen: {send_resp.message}")


async def send_to_matrix(profile: Profile, text: str) -> None:
    if MATRIX_SENDER is None:
        raise RuntimeError("Matrix sender ist nicht initialisiert")
    if not profile.mtx_room:
        raise ValueError("mtx_room fehlt im Profil")
    await MATRIX_SENDER.send_html(profile.mtx_room, text)


async def worker_task(name: str) -> None:
    profile = get_profile(name)
    time_window = profile.time_window_sec
    grouped: dict[str, list[EventData]] = {}
    last_event_time = 0.0
    has_events = False

    queue = PENDING[name]
    while True:
        await asyncio.sleep(0.1)
        now = time.time()

        try:
            data = queue.get_nowait()
        except asyncio.QueueEmpty:
            data = None

        if data is not None:
            msg_key = ""
            telegram = data.raw.get("telegram")
            if isinstance(telegram, dict):
                msg_key = str(telegram.get("message", "") or "")
                msg_key = escape_discord_markdown(msg_key) or ""

            grouped.setdefault(msg_key, []).append(data)
            last_event_time = now
            has_events = True

        if has_events and (now - last_event_time) >= time_window:
            for events in grouped.values():
                if not events:
                    continue

                text = format_output(events, profile)
                sender = profile.sender or "none"

                try:
                    if "rocketchat" in sender:
                        await send_to_rocketchat(profile, text)
                    if "telegram" in sender:
                        await send_to_telegram(profile, text)
                    if "pushover" in sender:
                        await send_to_pushover(profile, text)
                    if "discordWh" in sender:
                        await send_to_discord_webhook(profile, text)
                    if "matrix" in sender:
                        await send_to_matrix(profile, text)

                    if sender == "none" or (
                        "rocketchat" not in sender
                        and "telegram" not in sender
                        and "pushover" not in sender
                        and "discordWh" not in sender
                        and "matrix" not in sender
                    ):
                        print(f"Keinen gueltigen sender definiert! - {sender}")
                except Exception as exc:
                    print(f"Fehler beim Senden: {exc}")

                print(text)
                await write_log(profile, text)

            grouped.clear()
            has_events = False
            last_event_time = 0.0


def dispatch(event_data: EventData) -> None:
    name = event_data.name
    if name not in WORKER_TASKS:
        PENDING[name] = asyncio.Queue()
        WORKER_TASKS[name] = asyncio.create_task(worker_task(name))
    PENDING[name].put_nowait(event_data)


def extract_events_from_buffer(buffer: str) -> tuple[list[str], str]:
    events: list[str] = []
    current = buffer

    while True:
        start = current.find("<event")
        end = current.find("</event>")
        if start == -1 or end == -1:
            break
        end += len("</event>")
        events.append(current[start:end])
        current = current[end:]

    return events, current


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    remote = writer.get_extra_info("peername")
    print(f"Neue Verbindung von {remote}")

    buffer = ""
    try:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break

            buffer += chunk.decode("utf-8", errors="ignore")
            events, remaining = extract_events_from_buffer(buffer)
            buffer = remaining

            for xml_data in events:
                try:
                    event_data = parse_xml(xml_data)
                    dispatch(event_data)
                except Exception as exc:
                    preview = xml_data[:200] + "..." if len(xml_data) > 200 else xml_data
                    print(f"Fehler beim Verarbeiten: {exc}\nXML: {preview}")
    except Exception as exc:
        print(f"Fehler beim Lesen: {exc}")
    finally:
        writer.close()
        await writer.wait_closed()


async def tcp_server() -> None:
    assert CONFIG is not None

    hwid = f"{platform.node()}-{socket.gethostname()}"
    server = await asyncio.start_server(handle_client, CONFIG.tcp_host, CONFIG.tcp_port)

    print(f"Deine Hardware ID: {hwid}\n")
    print(f"TCP Server laeuft auf {CONFIG.tcp_host}:{CONFIG.tcp_port}\n")

    async with server:
        await server.serve_forever()


async def create_matrix_client() -> None:
    assert CONFIG
    global MATRIX_SENDER, MTX_CLIENT

    MATRIX_SENDER = MatrixSender(CONFIG)
    await MATRIX_SENDER.connect()
    MTX_CLIENT = MATRIX_SENDER.client


async def close_matrix_client() -> None:
    global MATRIX_SENDER, MTX_CLIENT
    if MATRIX_SENDER is not None:
        await MATRIX_SENDER.close()
    MATRIX_SENDER = None
    MTX_CLIENT = None


async def main() -> None:
    global CONFIG

    config_path = Path("config.json")
    if not config_path.exists():
        raise FileNotFoundError("config.json nicht gefunden (im Python-Ordner erwartet)")

    CONFIG = load_config(config_path)

    matrix_enabled = bool(CONFIG.mtx_homeserver and (CONFIG.mtx_access_token or (CONFIG.mtx_user and CONFIG.mtx_passwd)))
    if matrix_enabled:
        await create_matrix_client()

    try:
        await tcp_server()
    finally:
        await close_matrix_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
