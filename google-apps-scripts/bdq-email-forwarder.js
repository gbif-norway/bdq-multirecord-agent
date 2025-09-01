/**
 * Gmail → HTTPS forwarder (polls ALL new mail and POSTs JSON to your endpoint).
 *
 * Setup:
 * 1) In Apps Script editor: Services → enable "Gmail API" (Advanced Gmail Service).
 * 2) Edit ENDPOINT_URL and SHARED_SECRET below.
 * 3) Triggers → Add Trigger: choose poll, Event source: Time-driven, Every minute.
 * 4) First run: execute init() once to set the starting historyId.
 */

const ENDPOINT_URL = 'https://bdq-multirecord-agent-638241344017.europe-west1.run.app/email/incoming';
const SHARED_SECRET = ''; // optional HMAC; set '' to disable
const LABEL_PROCESSED = 'bdq/processed';
const MAX_PER_RUN = 50;      // safety cap per minute
const SAFETY_LOOKBACK = '2d';// extra search window
const MAX_SAFETY = 200;      // cap for safety search

function log() {
  try {
    const parts = Array.prototype.slice.call(arguments).map(a =>
      typeof a === 'string' ? a : JSON.stringify(a)
    );
    console.log(parts.join(' '));
  } catch (e) {}
}

function init() {
  const profile = Gmail.Users.getProfile('me');
  const startHistory = Gmail.Users.Messages.list('me', {maxResults: 1});
  const current = Gmail.Users.GetProfile ? Gmail.Users.GetProfile('me') : profile;

  let historyId;
  if (startHistory.messages && startHistory.messages.length) {
    const msg = Gmail.Users.Messages.get('me', startHistory.messages[0].id);
    historyId = msg.historyId;
  } else {
    historyId = '';
  }
  PropertiesService.getScriptProperties().setProperty('lastHistoryId', historyId);
  ensureLabel(LABEL_PROCESSED);
  log('init:set lastHistoryId', historyId || '(empty)');
}

function poll() {
  log('poll:start', new Date().toISOString());
  const props = PropertiesService.getScriptProperties();
  const lastHistoryId = props.getProperty('lastHistoryId') || '';
  log('poll:lastHistoryId', lastHistoryId || '(empty)');

  if (!lastHistoryId) {
    log('poll:first-run catch-up');
    const ids = listRecentInboxIds_(MAX_PER_RUN * 10);
    log('poll:first-run backlog ids', ids.length, ids.slice(0, 20));
    processMessageIds_(ids);
    const newest = ids[0]
      ? Gmail.Users.Messages.get('me', ids[0]).historyId
      : getCurrentHistoryId_();
    if (newest) props.setProperty('lastHistoryId', String(newest));
    log('poll:end:first-run set historyId', newest || '(none)');
    return;
  }

  // History since lastHistoryId
  const collectedIds = [];
  let pageToken = null;
  let processed = 0;

  do {
    const req = {
      startHistoryId: lastHistoryId,
      labelId: 'INBOX',
      pageToken: pageToken,
      historyTypes: ['messageAdded']
    };
    const hist = Gmail.Users.History.list('me', req);
    if (hist.history) {
      for (const h of hist.history) {
        if (h.messagesAdded) {
          for (const m of h.messagesAdded) {
            collectedIds.push(m.message.id);
            processed++;
            if (processed >= MAX_PER_RUN) break;
          }
        }
        if (processed >= MAX_PER_RUN) break;
      }
    }
    pageToken = hist.nextPageToken || null;
  } while (pageToken && processed < MAX_PER_RUN);

  log('poll:history collected', collectedIds.length);

  // Safety pass: recent unprocessed INBOX
  const safety = findUnprocessedRecentIds_(SAFETY_LOOKBACK, MAX_SAFETY);
  log('poll:safety ids', safety.length);

  // Union + dedupe
  const ids = uniqueIds_(collectedIds.concat(safety));
  log('poll:unique ids to process', ids.length, ids.slice(0, 20));

  if (ids.length) processMessageIds_(ids);

  // Advance cursor
  let newHistoryId = lastHistoryId;
  if (ids.length) {
    let maxH = BigInt(lastHistoryId || '0');
    for (const id of ids) {
      const msg = Gmail.Users.Messages.get('me', id, {format: 'minimal'});
      if (msg.historyId && BigInt(msg.historyId) > maxH) maxH = BigInt(msg.historyId);
    }
    newHistoryId = String(maxH);
  } else {
    const current = getCurrentHistoryId_();
    if (current) newHistoryId = String(current);
  }
  props.setProperty('lastHistoryId', newHistoryId);
  log('poll:end newHistoryId', newHistoryId);
}

/* ---------------- helpers ---------------- */

function replyNoAttachment_(message, headers) {
  const thread = GmailApp.getThreadById(message.threadId);
  const note =
    "Hi — no attachment was found.\n\n" +
    "Please send a NEW email with your data file attached (CSV/TSV or ZIP/DwC-A).\n" +
    "Replies without an attachment won’t be processed.\n";
  thread.reply(note);
}

function processMessageIds_(ids) {
  const labelIdProcessed = ensureLabel(LABEL_PROCESSED);
  log('process:start count', ids.length);

  for (const id of ids) {
    try {
      const meta = Gmail.Users.Messages.get('me', id, {format: 'minimal'});
      if ((meta.labelIds || []).includes(labelIdProcessed)) {
        log('process:skip already processed', id);
        continue;
      }

      const full = Gmail.Users.Messages.get('me', id, {format: 'full'});
      const payload = buildPayload_(full);
      const hdr = payload.headers || {};
      const subj = hdr.subject || '';
      const from = hdr.from || '';
      const attCount = (payload.attachments || []).length;

      log('process:msg', { id, subj, from, attCount });

      let ok = true;
      if (attCount === 0) {
        log('process:reply no attachment', id);
        replyNoAttachment_(full, hdr);
      } else {
        ok = postToEndpoint_(payload); // logs on non-2xx
      }

      if (ok) {
        Gmail.Users.Messages.modify({ addLabelIds: [labelIdProcessed] }, 'me', id);
        log('process:labeled processed', id);
      }
    } catch (e) {
      log('process:error', { id, error: String(e && e.message || e) });
    }
  }
  log('process:end');
}

function listRecentInboxIds_(limit) {
  const q = 'in:inbox';
  const out = [];
  let pageToken = null;
  do {
    const res = Gmail.Users.Messages.list('me', {q, maxResults: 100, pageToken});
    if (res.messages) out.push(...res.messages.map(m => m.id));
    pageToken = res.nextPageToken || null;
  } while (pageToken && out.length < limit);
  return out.slice(0, limit);
}

function getCurrentHistoryId_() {
  const res = Gmail.Users.Messages.list('me', {maxResults: 1});
  if (!res.messages || !res.messages.length) return '';
  const msg = Gmail.Users.Messages.get('me', res.messages[0].id, {format: 'minimal'});
  return msg.historyId || '';
}

function ensureLabel(name) {
  const labelsRes = Gmail.Users.Labels.list('me');
  const found = (labelsRes.labels || []).find(l => l.name === name);
  if (found) return found.id;
  const created = Gmail.Users.Labels.create({
    name,
    labelListVisibility: 'labelShow',
    messageListVisibility: 'show'
  }, 'me');
  return created.id;
}

function buildPayload_(message) {
  const headers = indexHeaders_(message.payload.headers || []);
  const parts = flattenParts_(message.payload);
  const {text, html} = extractBodies_(parts, message.id);

  const attachments = [];
  for (const p of parts) {
    if (p.filename && p.body && p.body.attachmentId) {
      const att = Gmail.Users.Messages.Attachments.get('me', message.id, p.body.attachmentId);
      let dataStr = (att && typeof att.data === 'string') ? att.data : '';
      let usedFallback = false;
      if (!dataStr) {
        // Fallback to GmailApp (blobs) if Advanced Gmail API returns empty data
        try {
          const msg = GmailApp.getMessageById(String(message.id));
          const blobs = msg.getAttachments({ includeInlineImages: true, includeAttachments: true });
          const match = blobs.find(b => {
            try {
              return b && b.getName && b.getName() === p.filename;
            } catch (e) { return false; }
          });
          if (match) {
            dataStr = Utilities.base64Encode(match.getBytes()); // standard base64 (server can handle both)
            usedFallback = true;
          }
        } catch (e) {
          log('attach:fallback error', String(e && e.message || e));
        }
      }
      log('attach:get', { filename: p.filename, size: (att && att.size) || 0, hasData: !!dataStr, fallback: usedFallback });
      attachments.push({
        filename: p.filename,
        mimeType: p.mimeType || 'application/octet-stream',
        size: (att && att.size) || (match && match.getBytes && match.getBytes().length) || 0,
        contentBase64: dataStr // base64 or base64url
      });
    }
  }

  return {
    receivedAt: new Date().toISOString(),
    messageId: message.id,
    threadId: message.threadId,
    historyId: message.historyId,
    labelIds: message.labelIds || [],
    snippet: message.snippet || '',
    headers: {
      subject: headers['Subject'] || '',
      from: headers['From'] || '',
      to: headers['To'] || '',
      cc: headers['Cc'] || '',
      date: headers['Date'] || '',
      messageId: headers['Message-Id'] || '',
      inReplyTo: headers['In-Reply-To'] || '',
      references: headers['References'] || ''
    },
    body: {
      text, // UTF-8 text if present
      html   // HTML if present
    },
    attachments
  };
}

function indexHeaders_(arr) {
  const map = {};
  for (const h of arr) map[h.name] = h.value;
  return map;
}

function flattenParts_(node) {
  const out = [];
  function walk(p) {
    if (!p) return;
    if (p.parts && p.parts.length) {
      p.parts.forEach(walk);
    } else {
      out.push(p);
    }
  }
  walk(node);
  return out;
}

function extractBodies_(parts, messageId) {
  let text = '', html = '';
  for (const p of parts) {
    const mime = (p.mimeType || '').toLowerCase();
    const isText = mime.startsWith('text/plain');
    const isHtml = mime.startsWith('text/html');
    if (!isText && !isHtml) continue;

    let dataStr = (p.body && typeof p.body.data === 'string') ? p.body.data : null;

    if (!dataStr && p.body && p.body.attachmentId) {
      const att = Gmail.Users.Messages.Attachments.get('me', messageId, p.body.attachmentId);
      if (att && typeof att.data === 'string') dataStr = att.data;
    }

    if (!dataStr) continue;
    const decoded = decodeBase64Url_(dataStr);
    if (isText) text = decoded;
    if (isHtml) html = decoded;
  }
  return { text, html };
}

function decodeBase64Url_(data) {
  if (typeof data !== 'string' || !data) return '';
  let s = data.replace(/-/g, '+').replace(/_/g, '/');
  const pad = s.length % 4;
  if (pad) s += '='.repeat(4 - pad);
  const bytes = Utilities.base64Decode(s);
  return Utilities.newBlob(bytes).getDataAsString('UTF-8');
}

function postToEndpoint_(payload) {
  const json = JSON.stringify(payload);
  const headers = {
    'Content-Type': 'application/json',
    'X-Gmail-Message-Id': payload.messageId || '',
    'X-Gmail-Thread-Id': payload.threadId || '',
    'X-From': (payload.headers && payload.headers.from) || '',
    'X-Subject': (payload.headers && payload.headers.subject) || ''
  };
  if (SHARED_SECRET) {
    const sigBytes = Utilities.computeHmacSha256Signature(json, SHARED_SECRET);
    headers['X-Signature'] = Utilities.base64Encode(sigBytes);
  }
  try {
    const res = UrlFetchApp.fetch(ENDPOINT_URL, {
      method: 'post',
      muteHttpExceptions: true,
      payload: json,
      headers,
      followRedirects: true,
      contentType: 'application/json'
    });
    const code = res.getResponseCode();
    if (code < 200 || code >= 300) {
      const text = (res.getContentText() || '').slice(0, 500);
      log('post:non2xx', code, text);
    } else {
      log('post:ok', code);
    }
    return code >= 200 && code < 300;
  } catch (e) {
    log('post:error', String(e && e.message || e));
    return false;
  }
}

function uniqueIds_(arr) {
  return Array.from(new Set(arr));
}

function findUnprocessedRecentIds_(window, limit) {
  const q = `-label:"${LABEL_PROCESSED}" -in:spam -in:trash newer_than:${window}`;
  const out = [];
  let pageToken = null;
  do {
    const res = Gmail.Users.Messages.list('me', {q, maxResults: 100, pageToken});
    if (res.messages) out.push(...res.messages.map(m => m.id));
    pageToken = res.nextPageToken || null;
  } while (pageToken && out.length < limit);
  return out.slice(0, limit);
}
