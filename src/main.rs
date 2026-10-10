//! `clip` module — listens for the `!clip` chat command and timestamps the
//! current stream position.
//!
//! Flow: the engine routes a `!clip` message to this module (it is the owner of
//! the command). The module looks up the platform's STREAM-START event in the
//! timeline database (the `[stream-start] ...` records the twitch/kick/youtube
//! adapters persist), computes `now - stream_start`, and:
//!   - if the stream hasn't started (no event yet, or now < start): flags the
//!     message via a ChatMessageRejected ("no stream to timestamp");
//!   - otherwise: records a clip event to the timeline with the HH:MM:SS
//!     timestamp.
//!
//! The message itself is always acked so the pipeline advances normally; the
//! rejection/clip decision is emitted after the ack.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

const COMMAND_NAME: &str = "clip";
const DEFAULT_FLAG: &str = "!";
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;
const CSV_FILE: &str = "clips.csv";

/// Resolve the default clip-export directory. Prefers `$HOME/.cockatiel/clips`
/// (outside the repo), falling back to `./.cockatiel/clips` when `$HOME` is
/// unset or empty. Pure so it is unit-testable.
fn default_clip_dir(home: Option<String>) -> String {
    match home {
        Some(h) if !h.is_empty() => format!("{}/.cockatiel/clips", h),
        _ => "./.cockatiel/clips".to_string(),
    }
}

/// Module config convention: settings live in config.json's `module_specific`
/// and are created (with defaults) when missing. Reads the configured command
/// flag plus the reconnect backoff bounds, backfilling defaults for any missing
/// key.
#[derive(Debug, Clone)]
struct ModuleSettings {
    command_flag: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
    clip_dir: String,
}

fn ensure_defaults() -> ModuleSettings {
    let root: Option<serde_json::Value> = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok());
    let ms = root
        .as_ref()
        .and_then(|r| r.get("module_specific"))
        .and_then(|ms| ms.as_object())
        .cloned()
        .unwrap_or_default();
    let command_flag = ms
        .get("command_flag")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| DEFAULT_FLAG.to_string());
    let reconnect_base_secs = ms
        .get("reconnect_base_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms
        .get("reconnect_max_secs")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_RECONNECT_MAX_SECS);
    let clip_dir = ms
        .get("clip_dir")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| default_clip_dir(std::env::var("HOME").ok()));
    if let Some(mut root) = root {
        if let Some(obj) = root.as_object_mut() {
            if let Some(ms) = obj
                .entry("module_specific".to_string())
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                if !ms.contains_key("command_flag") {
                    ms.insert("command_flag".into(), serde_json::json!(command_flag));
                }
                if !ms.contains_key("reconnect_base_secs") {
                    ms.insert("reconnect_base_secs".into(), serde_json::json!(reconnect_base_secs));
                }
                if !ms.contains_key("reconnect_max_secs") {
                    ms.insert("reconnect_max_secs".into(), serde_json::json!(reconnect_max_secs));
                }
                if !ms.contains_key("clip_dir") {
                    ms.insert("clip_dir".into(), serde_json::json!(clip_dir));
                }
                let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
            }
        }
    }
    ModuleSettings {
        command_flag,
        reconnect_base_secs,
        reconnect_max_secs,
        clip_dir,
    }
}

/// The module's engine identity (auth token + instance uuid + name), refreshed
/// on every reconnect so sends always carry the current session.
#[derive(Debug, Clone)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

async fn send_container(write_shared: &Arc<AsyncMutex<WsWriteHalf>>, container: ContainerForEngine) {
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_shared.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

/// Register the `!clip` chat command with the engine. Called on every fresh
/// session (initial connect and each reconnect — the engine forgets a session's
/// commands when the socket drops).
async fn register_commands(
    write_shared: &Arc<AsyncMutex<WsWriteHalf>>,
    identity: &Arc<AsyncMutex<EngineIdentity>>,
    command_flag: &str,
) {
    let id = identity.lock().await.clone();
    let commands = ContainerForEngine {
        version: 2,
        auth_token: id.auth,
        module_name: id.module,
        module_instance_uuid7: id.instance,
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![Command {
                command_name: COMMAND_NAME.to_string(),
                command_flag: command_flag.to_string(),
                command_description: "clip — timestamp the stream at its current position".to_string(),
                command_flags: vec![],
            }],
            alert_on_unknown_command: false,
        })),
    };
    send_container(write_shared, commands).await;
    info!("registered !{} command", COMMAND_NAME);
}

// ── Stream-start lookup + timestamp helpers ──────────────────────────────

/// Parse an ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.fraction](Z|±HH:MM)`)
/// into Unix seconds. The adapters persist stream-start times in this shape
/// (Twitch `started_at`, Kick `created_at`, YouTube `actualStartTime`).
fn parse_iso_utc(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-' || b[7] != b'-' || b[10] != b'T'
        || b[13] != b':' || b[16] != b':'
    {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    let month: i64 = s[5..7].parse().ok()?;
    let day: i64 = s[8..10].parse().ok()?;
    let hour: i64 = s[11..13].parse().ok()?;
    let min: i64 = s[14..16].parse().ok()?;
    let sec: i64 = s[17..19].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 61 {
        return None;
    }

    // Remainder after seconds: optional `.fraction`, then Z or ±HH:MM.
    let mut rest = &s[19..];
    if rest.starts_with('.') {
        let end = rest
            .find(['Z', '+', '-'])
            .unwrap_or(rest.len());
        rest = &rest[end..];
    }
    let mut offset_secs = 0i64;
    if !rest.is_empty() && !rest.starts_with('Z') {
        let off = rest;
        if off.len() >= 6 && (off.starts_with('+') || off.starts_with('-')) {
            let sign = if off.starts_with('-') { -1 } else { 1 };
            let oh: i64 = off[1..3].parse().ok()?;
            let om: i64 = off[4..6].parse().ok()?;
            offset_secs = sign * (oh * 3600 + om * 60);
        } else {
            return None;
        }
    }

    // Days since 1970-01-01 (proleptic Gregorian), then epoch seconds.
    let days = days_from_civil(year, month, day);
    Some(days * 86400 + hour * 3600 + min * 60 + sec - offset_secs)
}

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Format an elapsed-seconds duration as `HH:MM:SS`.
fn fmt_ts(secs: i64) -> String {
    let s = secs.max(0);
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let sec = s % 60;
    format!("{:02}:{:02}:{:02}", h, m, sec)
}

/// Extract the ISO start time from a `[stream-start] ... went live at <ISO> ...`
/// timeline message. Returns None if it can't be found/parsed.
fn extract_start_time(msg: &str) -> Option<i64> {
    let needle = "went live at ";
    let idx = msg.find(needle)?;
    let rest = msg[idx + needle.len()..].trim_start();
    let iso = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(|c: char| c == '"' || c == '—' || c == '–' || c == '-');
    parse_iso_utc(iso)
}

/// Is the incoming routed message actually the `!clip` command? The engine
/// routes commands to the owning module via targeted routing, but the module
/// also sits in the pre-process fanout for non-command messages — so only act
/// when the engine attached a parsed `clip` command (or the raw text starts
/// with the flag+name).
fn is_clip_command(chat: Option<&ChatMessage>, flag: &str) -> bool {
    if let Some(c) = chat {
        if let Some(cmd) = &c.command {
            if cmd.command_name == COMMAND_NAME {
                return true;
            }
        }
        let raw = c.raw_message.trim_start();
        if raw.starts_with(&format!("{}{}", flag, COMMAND_NAME)) {
            return true;
        }
    }
    false
}

/// A `TimelineQuery` to find the platform's most recent stream-start event.
/// The adapters persist them via log_to_timeline → archival events whose
/// raw_message begins `[stream-start] <platform>: ...`. This is a typed query,
/// not raw SQL.
fn stream_start_query(platform: &str, request_id: &str) -> TimelineQuery {
    TimelineQuery {
        timeline_id_uuid7: String::new(),
        request_id: request_id.to_string(),
        event_type: 0,
        platform: platform.to_string(),
        user_uuid7: String::new(),
        kind: String::new(),
        raw_prefix: format!("[stream-start] {}:", platform),
        pipeline_status: String::new(),
        since_ms: 0,
        limit: 1,
        offset: 0,
    }
}

// ── Chat reply + CSV export helpers ──────────────────────────────────────

/// Build the `chat_reply` virtual-query payload: the engine (not the module)
/// authors the message, so we only hand it platform/channel/message.
fn chat_reply_payload(platform: &str, channel_id: &str, message: &str) -> String {
    serde_json::json!({
        "platform": platform,
        "channel_id": channel_id,
        "message": message,
    })
    .to_string()
}

/// The CSV export header. Columns: offset, platform, user uuid7, unix seconds.
fn csv_header() -> &'static str {
    "stream_offset,platform,user,unix_time\n"
}

/// Quote a CSV field iff it contains a delimiter, quote, or newline (RFC 4180).
fn csv_field(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Format one CSV export row (including the trailing newline).
fn csv_row(ts: &str, platform: &str, user: &str, unix: i64) -> String {
    format!(
        "{},{},{},{}\n",
        csv_field(ts),
        csv_field(platform),
        csv_field(user),
        unix
    )
}

/// Append a row to `<dir>/clips.csv`, creating the directory and (when the file
/// is new/empty) writing the header first. Opens in append mode and flushes so
/// the export is current even if the process crashes mid-stream.
fn append_clip_csv(dir: &str, row: &str) -> std::io::Result<()> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let path = std::path::Path::new(dir).join(CSV_FILE);
    let new_or_empty = std::fs::metadata(&path).map(|m| m.len() == 0).unwrap_or(true);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    if new_or_empty {
        file.write_all(csv_header().as_bytes())?;
    }
    file.write_all(row.as_bytes())?;
    file.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let module_name = client.config.module_name.clone();
    info!("clip module connected as '{}'", module_name);
    let identity: Arc<AsyncMutex<EngineIdentity>> = Arc::new(AsyncMutex::new(EngineIdentity {
        auth: client.auth_token.clone(),
        instance: client.instance_uuid7.clone(),
        module: module_name,
    }));

    let settings = ensure_defaults();
    let command_flag = settings.command_flag.clone();
    let reconnect_base_secs = settings.reconnect_base_secs;
    let reconnect_max_secs = settings.reconnect_max_secs;
    let clip_dir_for_task = settings.clip_dir.clone();
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);
    let command_flag_for_task = command_flag.clone();

    tokio::spawn(async move {
        let mut read = read;
        // A `!clip` command whose stream-start query is still in flight: we ack
        // the message immediately, then emit the rejection or clip event when
        // the DatabaseQueryResult arrives (matched by query_id).
        let mut pending: Option<PendingClip> = None;
        let mut backoff = reconnect_base_secs;
        loop {
            // Fresh session (initial connect + every reconnect): register the
            // command — the engine forgets commands when a socket drops.
            register_commands(&write_for_task, &identity_for_task, &command_flag_for_task).await;
            loop {
                let Some(msg) = read.next().await else { break };
                let data = match msg {
                    Ok(WsMessage::Binary(d)) => d,
                    Ok(WsMessage::Close(_)) => {
                        info!("Engine closed connection");
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("Engine WebSocket error: {}", e);
                        break;
                    }
                };
                let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                let id = identity_for_task.lock().await.clone();
                match container.payload {
                    Some(ModulePayload::AuthVerify(_)) => {
                        let reply = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                cur_auth: id.auth.clone(),
                            })),
                        };
                        send_container(&write_for_task, reply).await;
                    }
                    Some(ModulePayload::TimelineQueryResult(res)) => {
                        if let Some(p) = pending.take() {
                            if p.request_id == res.request_id {
                                handle_clip_result(
                                    &write_for_task,
                                    &identity_for_task,
                                    &clip_dir_for_task,
                                    p,
                                    &res,
                                )
                                .await;
                            } else {
                                pending = Some(p);
                            }
                        }
                    }
                    Some(ModulePayload::MessagePreProcess(pre)) => {
                        if !pre.message_uuid7.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: pre.message_uuid7.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        // Ack every pre-process message so the pipeline advances
                        // (echo the raw message back with the same uuid).
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
                                message_uuid7: pre.message_uuid7.clone(),
                                raw_message: pre.raw_message.clone(),
                                audio: Vec::new(),
                                audio_type: String::new(),
                            })),
                        };
                        send_container(&write_for_task, ack).await;

                        // Act only on the routed `!clip` command.
                        if is_clip_command(pre.raw_message.as_ref(), &command_flag_for_task)
                            && !pre.message_uuid7.is_empty()
                        {
                            let platform = pre
                                .raw_message
                                .as_ref()
                                .map(|c| c.platform.clone())
                                .unwrap_or_default();
                            if platform.is_empty() {
                                continue;
                            }
                            let request_id = uuid::Uuid::new_v4().to_string();
                            let query = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::TimelineQuery(
                                    stream_start_query(&platform, &request_id),
                                )),
                            };
                            send_container(&write_for_task, query).await;
                            pending = Some(PendingClip {
                                request_id,
                                uuid: pre.message_uuid7,
                                platform,
                                user: pre
                                    .raw_message
                                    .as_ref()
                                    .map(|c| c.user_uuid7.clone())
                                    .unwrap_or_default(),
                                channel_id: pre
                                    .raw_message
                                    .as_ref()
                                    .map(|c| c.channel_id.clone())
                                    .unwrap_or_default(),
                            });
                        }
                    }
                    _ => {}
                }
            }
            // Reconnect with exponential backoff (1s, 2s, 4s … capped).
            info!("reconnecting in {}s", backoff);
            tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
            backoff = (backoff * 2).min(reconnect_max_secs);
        }
    });

    // Keep the process alive; the supervisor monitors liveness.
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

/// A `!clip` command awaiting its stream-start query result.
struct PendingClip {
    request_id: String,
    uuid: String,
    platform: String,
    user: String,
    channel_id: String,
}

/// Decide + emit for a `!clip` once the stream-start query result arrives.
async fn handle_clip_result(
    write_shared: &Arc<AsyncMutex<WsWriteHalf>>,
    identity: &Arc<AsyncMutex<EngineIdentity>>,
    clip_dir: &str,
    p: PendingClip,
    res: &TimelineQueryResult,
) {
    let id = identity.lock().await.clone();
    let mut start_epoch: Option<i64> = None;
    if let Some(event) = res.events.first() {
        start_epoch = extract_start_time(&event.raw_message);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    match start_epoch {
        Some(start) if now >= start => {
            let ts = fmt_ts(now - start);
            let clip = ContainerForEngine {
                version: 2,
                auth_token: id.auth.clone(),
                module_name: id.module.clone(),
                module_instance_uuid7: id.instance.clone(),
                payload: Some(EnginePayload::Log(Log {
                    log: format!(
                        "[clip] {}: timestamp {} (stream started {}, user {})",
                        p.platform, ts, start, p.user
                    ),
                    blob: vec![],
                })),
            };
            send_container(write_shared, clip).await;
            info!("[clip] {} timestamp {}", p.platform, ts);

            // Reply in chat (engine-authored) with the retrievable timestamp.
            let reply = ContainerForEngine {
                version: 2,
                auth_token: id.auth.clone(),
                module_name: id.module.clone(),
                module_instance_uuid7: id.instance.clone(),
                payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                    query_id: "chat_reply".to_string(),
                    sql: chat_reply_payload(
                        &p.platform,
                        &p.channel_id,
                        &format!("clip marked at {} into the stream", ts),
                    ),
                    params: vec![],
                })),
            };
            send_container(write_shared, reply).await;

            // Append the export row immediately so a crash never loses it.
            let row = csv_row(&ts, &p.platform, &p.user, now);
            if let Err(e) = append_clip_csv(clip_dir, &row) {
                warn!("[clip] failed to append CSV export: {}", e);
            }
        }
        _ => {
            // Stream hasn't started (or no stream-start event): flag the
            // message as rejected so the operator sees "no stream to
            // timestamp".
            let rej = ChatMessageRejected {
                message_uuid7: p.uuid.clone(),
                message: None,
                processed_message: Some(String::new()),
                reason: "no stream to timestamp".to_string(),
                origin: "clip".to_string(),
            };
            let reject = ContainerForEngine {
                version: 2,
                auth_token: id.auth.clone(),
                module_name: id.module.clone(),
                module_instance_uuid7: id.instance.clone(),
                payload: Some(EnginePayload::ChatMessageRejected(rej)),
            };
            send_container(write_shared, reject).await;
            info!("[clip] rejected '{}': no stream to timestamp", p.platform);

            // Reply in chat (engine-authored) so the user sees why.
            let reply = ContainerForEngine {
                version: 2,
                auth_token: id.auth.clone(),
                module_name: id.module.clone(),
                module_instance_uuid7: id.instance.clone(),
                payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                    query_id: "chat_reply".to_string(),
                    sql: chat_reply_payload(&p.platform, &p.channel_id, "no stream to timestamp"),
                    params: vec![],
                })),
            };
            send_container(write_shared, reply).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_iso_utc_z() {
        // 2026-09-24T20:00:00Z
        let expected = days_from_civil(2026, 9, 24) * 86400 + 20 * 3600;
        assert_eq!(parse_iso_utc("2026-09-24T20:00:00Z"), Some(expected));
    }

    #[test]
    fn parses_iso_utc_with_offset_and_fraction() {
        let base = days_from_civil(2026, 9, 24) * 86400 + 12 * 3600;
        // +08:00 → subtract 8h → 12:00 local = 04:00 UTC
        assert_eq!(parse_iso_utc("2026-09-24T12:00:00+08:00"), Some(base - 8 * 3600));
        // fraction ignored, Z suffix
        assert_eq!(parse_iso_utc("2026-09-24T20:00:00.123Z"), Some(base + 8 * 3600));
    }

    #[test]
    fn rejects_garbage_iso() {
        assert_eq!(parse_iso_utc("not-a-time"), None);
        assert_eq!(parse_iso_utc("2026-13-99T25:99:99Z"), None);
    }

    #[test]
    fn formats_timestamp_hms() {
        assert_eq!(fmt_ts(0), "00:00:00");
        assert_eq!(fmt_ts(3661), "01:01:01");
        assert_eq!(fmt_ts(90061), "25:01:01");
    }

    #[test]
    fn extracts_start_time_from_stream_start_message() {
        let msg = "[stream-start] twitch: channel 'vulbyte' went live at 2026-09-24T20:00:00Z — title: \"my stream\"";
        let expected = parse_iso_utc("2026-09-24T20:00:00Z");
        assert_eq!(extract_start_time(msg), expected);
        assert_eq!(extract_start_time("no stream event here"), None);
    }

    #[test]
    fn clip_command_detection() {
        let chat = ChatMessage {
            platform: "twitch".into(),
            raw_data: vec![],
            raw_message: "!clip".into(),
            user_uuid7: "u1".into(),
            command: Some(Command {
                command_name: "clip".into(),
                command_flag: "!".into(),
                command_description: String::new(),
                command_flags: vec![],
            }),
            channel_id: String::new(),
            user_data: None,
        };
        assert!(is_clip_command(Some(&chat), "!"));
        let other = ChatMessage {
            command: None,
            raw_message: "hello".into(),
            ..chat.clone()
        };
        assert!(!is_clip_command(Some(&other), "!"));
    }

    #[test]
    fn stream_start_query_targets_the_platform() {
        let q = stream_start_query("kick", "req-1");
        assert_eq!(q.request_id, "req-1");
        assert_eq!(q.platform, "kick");
        assert_eq!(q.raw_prefix, "[stream-start] kick:");
        assert_eq!(q.limit, 1);
    }

    #[test]
    fn chat_reply_payload_has_platform_channel_and_message() {
        let payload = chat_reply_payload("twitch", "chan-42", "clip marked at 00:01:02 into the stream");
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["platform"], "twitch");
        assert_eq!(v["channel_id"], "chan-42");
        assert_eq!(v["message"], "clip marked at 00:01:02 into the stream");
    }

    #[test]
    fn csv_header_shape() {
        assert_eq!(csv_header(), "stream_offset,platform,user,unix_time\n");
    }

    #[test]
    fn csv_row_shape() {
        assert_eq!(
            csv_row("00:01:02", "twitch", "uuid-1", 1_700_000_000),
            "00:01:02,twitch,uuid-1,1700000000\n"
        );
    }

    #[test]
    fn csv_row_escapes_commas_and_quotes() {
        // A field with a comma is quoted; embedded quotes are doubled.
        assert_eq!(
            csv_row("00:00:01", "twitch", "a,b", 1),
            "00:00:01,twitch,\"a,b\",1\n"
        );
        assert_eq!(
            csv_row("00:00:01", "twitch", "he said \"hi\"", 1),
            "00:00:01,twitch,\"he said \"\"hi\"\"\",1\n"
        );
    }

    #[test]
    fn clip_dir_default_resolution() {
        assert_eq!(
            default_clip_dir(Some("/home/streamer".to_string())),
            "/home/streamer/.cockatiel/clips"
        );
        assert_eq!(default_clip_dir(None), "./.cockatiel/clips");
        assert_eq!(default_clip_dir(Some(String::new())), "./.cockatiel/clips");
    }
}