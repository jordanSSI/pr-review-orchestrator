"""Fresh dashboard shell for the PR review coordinator web UI."""

from __future__ import annotations

import html
import json
from typing import Any


def _option(value: str, label: str, selected: str) -> str:
    selected_attr = " selected" if value == selected else ""
    return f'<option value="{html.escape(value)}"{selected_attr}>{html.escape(label)}</option>'


def render_dashboard_next_shell(
    *,
    scope: str,
    status_filter: str,
    sort_key: str,
    status_filters: list[str],
    default_refresh_interval_seconds: int,
    active_refresh_interval_seconds: int,
    new_thread_sentinel: str,
    navigation_html: str,
) -> str:
    scope_options = "".join(
        [
            _option("active", "Active", scope),
            _option("archived", "Archived", scope),
            _option("all", "All", scope),
        ]
    )
    status_options = _option("all", "All", status_filter) + "".join(
        _option(name, name, status_filter) for name in status_filters
    )
    sort_options = "".join(
        [
            _option("updated", "Updated", sort_key),
            _option("status", "Status", sort_key),
            _option("pr", "PR number/repo", sort_key),
            _option("last_poll", "Last poll", sort_key),
        ]
    )
    replacements = {
        "__ACTIVE_REFRESH_INTERVAL_SECONDS__": str(active_refresh_interval_seconds),
        "__DEFAULT_REFRESH_INTERVAL_SECONDS__": str(default_refresh_interval_seconds),
        "__NAVIGATION_HTML__": navigation_html,
        "__NEW_THREAD_SENTINEL__": json.dumps(new_thread_sentinel),
        "__SCOPE__": json.dumps(scope),
        "__SCOPE_OPTIONS__": scope_options,
        "__SORT__": json.dumps(sort_key),
        "__SORT_OPTIONS__": sort_options,
        "__STATUS__": json.dumps(status_filter),
        "__STATUS_OPTIONS__": status_options,
    }
    markup = _NEXT_DASHBOARD_TEMPLATE
    for marker, value in replacements.items():
        markup = markup.replace(marker, value)
    return markup


_NEXT_DASHBOARD_TEMPLATE = """
        <style>
          .next-page-header { position: sticky; top: 0; z-index: 20; background: #fbfcfd; border-bottom: 1px solid #d9dee7; box-shadow: 0 1px 3px rgba(15,23,42,0.06); }
          .next-dashboard { display: grid; grid-template-columns: minmax(300px, 24vw) minmax(0, 1fr); gap: 0; height: calc(100vh - 128px); min-height: 520px; padding: 0; background: #eef2f7; }
          .next-rail { border-right: 1px solid #d9dee7; background: #f8fafc; min-width: 0; min-height: 0; display: grid; grid-template-rows: auto minmax(0, 1fr); }
          .next-rail-head { padding: 14px; border-bottom: 1px solid #e3e8ef; display: grid; gap: 12px; }
          .next-rail-actions { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
          .next-filters { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; align-items: end; }
          .next-filters label { min-width: 0; }
          .next-filters select { width: 100%; min-width: 0; }
          .next-filters button { grid-column: span 2; width: 100%; }
          .next-tabs { overflow: auto; padding: 8px; display: grid; align-content: start; gap: 6px; }
          .next-pr-tab { width: 100%; min-height: 92px; padding: 10px; border: 1px solid transparent; border-radius: 8px; background: transparent; text-align: left; display: grid; gap: 6px; cursor: pointer; color: #1f2937; }
          .next-pr-tab:hover { background: #fff; border-color: #d9dee7; }
          .next-pr-tab.current { background: #fff; border-color: rgba(13,115,119,0.35); box-shadow: 0 1px 3px rgba(15,23,42,0.08); }
          .next-tab-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; min-width: 0; }
          .next-tab-title { min-width: 0; font-weight: 650; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
          .next-tab-subtitle { color: #6b7280; font-size: 12px; line-height: 1.35; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
          .next-tab-meta { color: #6b7280; font-size: 11px; line-height: 1.3; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
          .next-main { min-width: 0; min-height: 0; display: grid; grid-template-rows: auto minmax(0, 1fr) auto; background: #fff; }
          .next-empty { min-height: 320px; display: grid; place-items: center; color: #6b7280; }
          .next-record-head { padding: 16px 18px 14px; border-bottom: 1px solid #e3e8ef; display: grid; gap: 12px; background: #fff; }
          .next-title-row { display: flex; justify-content: space-between; gap: 16px; align-items: start; }
          .next-title-row h2 { margin: 0; font-size: 18px; line-height: 1.3; }
          .next-title-row a { font-weight: 650; }
          .next-meta-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
          .next-meta-item { min-width: 0; border: 1px solid #e3e8ef; border-radius: 8px; padding: 8px 10px; background: #f8fafc; }
          .next-meta-label { color: #6b7280; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; }
          .next-meta-value { margin-top: 2px; font-size: 12px; overflow-wrap: anywhere; }
          .next-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
          .next-history { min-height: 0; overflow: auto; padding: 18px; background: linear-gradient(180deg, #f8fafc 0%, #ffffff 140px); }
          .next-stream { max-width: 1040px; display: grid; gap: 10px; }
          .next-bubble { border: 1px solid #e3e8ef; border-radius: 8px; padding: 10px 12px; background: #fff; box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
          .next-bubble.agent { border-color: rgba(13,115,119,0.22); background: #f2fbfa; }
          .next-bubble.user { border-color: rgba(37,99,235,0.22); background: #f5f8ff; }
          .next-bubble.warning { border-color: rgba(146,64,14,0.22); background: #fffbeb; }
          .next-bubble-head { display: flex; justify-content: space-between; gap: 10px; color: #6b7280; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.07em; }
          .next-bubble-body { margin-top: 6px; color: #1f2937; font-size: 13px; white-space: pre-wrap; overflow-wrap: anywhere; }
          .next-activity-line { color: #4b5563; font-size: 12px; line-height: 1.45; white-space: pre-wrap; overflow-wrap: anywhere; }
          .next-activity-line + .next-activity-line { margin-top: 4px; }
          .next-thread-tools { margin-top: 4px; }
          .next-thread-tools summary { cursor: pointer; color: #0d7377; font-size: 12px; font-weight: 650; }
          .next-thread-panel { margin-top: 8px; display: grid; gap: 8px; max-width: 760px; }
          .next-thread-panel input { width: min(100%, 520px); }
          .next-thread-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: end; }
          .next-composer { border-top: 1px solid #d9dee7; padding: 12px 18px 14px; background: #fbfcfd; }
          .next-composer form { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; align-items: end; }
          .next-composer textarea { min-height: 68px; max-height: 180px; resize: vertical; width: 100%; border: 1px solid #cfd7e3; background: #fff; border-radius: 8px; padding: 9px 10px; font: inherit; font-size: 13px; color: #1f2937; }
          .next-composer textarea:focus { outline: none; box-shadow: 0 0 0 3px rgba(13,115,119,0.22); border-color: #0d7377; }
          .next-composer button { min-height: 40px; }
          .next-kv { display: grid; gap: 2px; }
          .next-kv code { font-size: 11px; }
          .next-hidden { display: none; }
          @media (max-width: 980px) {
            .next-dashboard { grid-template-columns: 1fr; grid-template-rows: auto 1fr; height: auto; min-height: calc(100vh - 128px); }
            .next-rail { border-right: 0; border-bottom: 1px solid #d9dee7; max-height: 46vh; }
            .next-meta-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          }
          @media (max-width: 720px) {
            .next-filters { grid-template-columns: 1fr; }
            .next-title-row, .next-composer form { grid-template-columns: 1fr; display: grid; }
            .next-meta-grid { grid-template-columns: 1fr; }
          }
        </style>
        <header class="page-header next-page-header">
          <div class="header-row">
            <div class="header-copy">
              <h1>PR Review Coordinator</h1>
              <p>Tracked PRs, attached agent threads, and queued follow-up in one workspace.</p>
            </div>
            <div class="header-controls">
              <div class="small" id="refresh-status">Loading dashboard...</div>
              <div class="button-row compact">
                <button type="button" id="refresh-toggle">Pause auto-refresh</button>
                <button type="button" id="refresh-now">Refresh now</button>
              </div>
            </div>
          </div>
          <div class="header-row header-row-secondary">
            __NAVIGATION_HTML__
            <div id="flash" class="flash hidden"></div>
          </div>
        </header>
        <main id="next-dashboard-root" class="next-dashboard">
          <aside class="next-rail">
            <div class="next-rail-head">
              <div class="next-rail-actions">
                <h2>Tracked PRs</h2>
                <button type="button" data-action="poll-all">Poll all</button>
              </div>
              <form id="dashboard-filters" class="next-filters">
                <label>Scope
                  <select name="scope">__SCOPE_OPTIONS__</select>
                </label>
                <label>Status
                  <select name="status">__STATUS_OPTIONS__</select>
                </label>
                <label>Sort
                  <select name="sort">__SORT_OPTIONS__</select>
                </label>
                <button type="submit">Apply</button>
              </form>
            </div>
            <div id="pr-tabs" class="next-tabs" role="tablist" aria-label="Tracked PRs">
              <div class="next-empty">Loading tracked PRs...</div>
            </div>
          </aside>
          <section id="conversation-pane" class="next-main">
            <div class="next-empty">Loading dashboard...</div>
          </section>
        </main>
        <script>
          const DASHBOARD_API_URL = '/api/dashboard';
          const ACTION_API_BASE = '/api/actions';
          const DEFAULT_REFRESH_INTERVAL_SECONDS = __DEFAULT_REFRESH_INTERVAL_SECONDS__;
          const ACTIVE_REFRESH_INTERVAL_SECONDS = __ACTIVE_REFRESH_INTERVAL_SECONDS__;
          const REFRESH_PAUSE_KEY = 'pr-review-coordinator.next.refresh-paused';
          const NEW_THREAD_SENTINEL = __NEW_THREAD_SENTINEL__;
          const initialParams = new URLSearchParams(window.location.search);
          const state = {
            filters: {
              scope: __SCOPE__,
              status: __STATUS__,
              sort: __SORT__,
            },
            selectedKey: initialParams.get('selected') || null,
            refreshPaused: sessionStorage.getItem(REFRESH_PAUSE_KEY) === '1',
            refreshIntervalSeconds: DEFAULT_REFRESH_INTERVAL_SECONDS,
            secondsRemaining: DEFAULT_REFRESH_INTERVAL_SECONDS,
            loading: false,
            records: [],
            jobs: [],
            events: [],
          };

          function escapeHtml(value) {
            return String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
          }

          function badgeClass(status) {
            if (['awaiting_final_review', 'awaiting_final_test', 'succeeded', 'ok'].includes(status)) return 'pill good';
            if (['merge_conflicts', 'needs_review', 'needs_ci_fix', 'pending_copilot_review', 'copilot_review_cooldown', 'running', 'queued', 'busy'].includes(status)) return 'pill warn';
            if (['error', 'failed', 'closed'].includes(status)) return 'pill bad';
            return 'pill';
          }

          function statusBadge(status) {
            const value = status || 'unknown';
            return `<span class="${badgeClass(value)}">${escapeHtml(value)}</span>`;
          }

          let flashTimer = null;
          function showFlash(message, tone='success') {
            const flash = document.getElementById('flash');
            if (!flash) return;
            if (flashTimer) {
              clearTimeout(flashTimer);
              flashTimer = null;
            }
            if (!message) {
              flash.textContent = '';
              flash.className = 'flash hidden';
              return;
            }
            flash.textContent = message;
            flash.className = tone === 'error' ? 'flash error' : 'flash success';
            if (tone !== 'error') {
              flashTimer = setTimeout(() => {
                flash.textContent = '';
                flash.className = 'flash hidden';
              }, 4000);
            }
          }

          function selectedRecord() {
            return (state.records || []).find((record) => record.key === state.selectedKey) || null;
          }

          function ensureSelection() {
            if (!state.records.length) {
              state.selectedKey = null;
              return;
            }
            if (!selectedRecord()) {
              state.selectedKey = state.records[0].key;
            }
          }

          function updateUrl() {
            const params = new URLSearchParams();
            params.set('scope', state.filters.scope);
            params.set('status', state.filters.status);
            params.set('sort', state.filters.sort);
            if (state.selectedKey) params.set('selected', state.selectedKey);
            history.replaceState(null, '', `/next?${params.toString()}`);
          }

          function updateRefreshControls() {
            const status = document.getElementById('refresh-status');
            const toggle = document.getElementById('refresh-toggle');
            if (status) status.textContent = state.refreshPaused ? 'Auto-refresh paused.' : `Auto-refresh in ${state.secondsRemaining}s.`;
            if (toggle) toggle.textContent = state.refreshPaused ? 'Resume auto-refresh' : 'Pause auto-refresh';
          }

          function nextRefreshInterval(records) {
            return (records || []).some((record) => {
              const runStatus = record.run_status || '';
              const activity = record.live_activity || {};
              const hasLiveActivity = !!(activity.headline || (Array.isArray(activity.items) && activity.items.length));
              return hasLiveActivity || ['running', 'busy'].includes(runStatus);
            }) ? ACTIVE_REFRESH_INTERVAL_SECONDS : DEFAULT_REFRESH_INTERVAL_SECONDS;
          }

          function renderTabs() {
            const tabs = document.getElementById('pr-tabs');
            if (!tabs) return;
            if (!state.records.length) {
              tabs.innerHTML = '<div class="next-empty">No matching tracked PRs</div>';
              return;
            }
            tabs.innerHTML = state.records.map((record) => {
              const current = record.key === state.selectedKey ? ' current' : '';
              const detail = record.detail_text ? `<div class="next-tab-meta">${escapeHtml(record.detail_text)}</div>` : '';
              return `
                <button type="button" class="next-pr-tab${current}" data-select-key="${escapeHtml(record.key)}" role="tab" aria-selected="${current ? 'true' : 'false'}">
                  <span class="next-tab-row">
                    <span class="next-tab-title">${escapeHtml(record.repo_name)} #${escapeHtml(record.pr_number)}</span>
                    ${statusBadge(record.status)}
                  </span>
                  <span class="next-tab-subtitle">${escapeHtml(record.pr_title)}</span>
                  <span class="next-tab-row">
                    <span class="next-tab-meta">${escapeHtml(record.branch)}</span>
                    ${statusBadge(record.run_status)}
                  </span>
                  ${detail}
                </button>
              `;
            }).join('');
          }

          function compactActionButtons(record) {
            const disabled = record.actions_disabled ? 'disabled' : '';
            return `
              <div class="next-actions" data-record-key="${escapeHtml(record.key)}">
                ${record.stop_available ? `<button type="button" data-action="stop-run" data-key="${escapeHtml(record.key)}">Hard stop</button>` : ''}
                <button type="button" data-action="poll-one" data-key="${escapeHtml(record.key)}" ${disabled}>Poll</button>
                ${record.dirty_worktree_busy ? `<button type="button" data-action="clear-worktree" data-key="${escapeHtml(record.key)}" ${disabled}>Clear worktree</button>` : ''}
                ${record.dirty_worktree_busy ? `<button type="button" data-action="use-worktree-anyway" data-key="${escapeHtml(record.key)}" ${disabled}>Use worktree anyway</button>` : ''}
                <button type="button" data-action="untrack" data-key="${escapeHtml(record.key)}" ${disabled}>Untrack</button>
                <button type="button" data-action="untrack-cleanup" data-key="${escapeHtml(record.key)}" ${disabled}>Untrack + cleanup</button>
              </div>
            `;
          }

          function threadTools(record) {
            const thread = record.thread || {};
            if (thread.provider !== 'codex') {
              return `
                <details class="next-thread-tools">
                  <summary>Provider thread</summary>
                  <div class="next-thread-panel">
                    <div class="next-kv">
                      <span class="small">Attached thread</span>
                      <code>${escapeHtml(thread.id || '')}</code>
                    </div>
                    <div class="small stack">${escapeHtml(thread.title || '')}</div>
                  </div>
                </details>
              `;
            }
            const disabled = record.actions_disabled ? 'disabled' : '';
            const options = (thread.recent_threads || []).map((item) => {
              const label = item.in_use_by ? `${item.summary} | in use by ${item.in_use_by}` : item.summary;
              return `<option value="${escapeHtml(item.id)}" label="${escapeHtml(label)}"></option>`;
            }).join('');
            return `
              <details class="next-thread-tools">
                <summary>Codex thread</summary>
                <div class="next-thread-panel" data-record-key="${escapeHtml(record.key)}">
                  <div class="next-kv">
                    <span class="small">Attached Codex thread</span>
                    <code>${escapeHtml(thread.id || '')}</code>
                  </div>
                  <div class="small stack">${escapeHtml(thread.title || '')}</div>
                  <div class="next-thread-row">
                    <label>Thread ID
                      <input type="text" data-role="thread-id-input" value="${escapeHtml(thread.id || '')}" list="thread-options-${escapeHtml(record.key)}" ${disabled}>
                    </label>
                    <datalist id="thread-options-${escapeHtml(record.key)}">${options}</datalist>
                    <button type="button" data-action="set-thread" data-key="${escapeHtml(record.key)}" ${disabled}>Set thread</button>
                    <button type="button" data-action="latest-thread" data-key="${escapeHtml(record.key)}" ${disabled}>Latest repo thread</button>
                    <button type="button" data-action="fresh-thread" data-key="${escapeHtml(record.key)}" ${disabled}>Fresh thread</button>
                  </div>
                </div>
              </details>
            `;
          }

          function metaItem(label, value, code=false) {
            const body = code ? `<code>${escapeHtml(value || '')}</code>` : escapeHtml(value || '');
            return `<div class="next-meta-item"><div class="next-meta-label">${escapeHtml(label)}</div><div class="next-meta-value">${body}</div></div>`;
          }

          function liveActivityBubbles(record) {
            const activity = record.live_activity || {};
            const bubbles = [];
            if (activity.headline) {
              const lines = (activity.items || []).map((item) => `<div class="next-activity-line">${escapeHtml(item.text || '')}</div>`).join('');
              bubbles.push(`
                <div class="next-bubble agent">
                  <div class="next-bubble-head"><span>Codex</span><span>${escapeHtml(record.live_activity_updated_label || '')}</span></div>
                  <div class="next-bubble-body">${escapeHtml(activity.headline)}${lines ? `<div style="margin-top:8px">${lines}</div>` : ''}</div>
                </div>
              `);
            }
            if (record.run_summary) {
              bubbles.push(`
                <div class="next-bubble">
                  <div class="next-bubble-head"><span>Run summary</span><span>${escapeHtml(record.run_detail_meta || '')}</span></div>
                  <div class="next-bubble-body">${escapeHtml(record.run_summary)}</div>
                </div>
              `);
            }
            return bubbles;
          }

          function jobBubbles(record) {
            return (state.jobs || [])
              .filter((job) => job.tracked_pr_key === record.key)
              .slice(0, 6)
              .map((job) => {
                if (job.action === 'steer-message' && job.payload_message) {
                  return `
                    <div class="next-bubble user">
                      <div class="next-bubble-head"><span>Queued message</span><span>${escapeHtml(job.requested_at_label || '')}</span></div>
                      <div class="next-bubble-body">${escapeHtml(job.payload_message)}</div>
                    </div>
                  `;
                }
                return `
                  <div class="next-bubble">
                    <div class="next-bubble-head"><span>Job ${escapeHtml(job.id)} | ${escapeHtml(job.action)}</span><span>${escapeHtml(job.requested_at_label || '')}</span></div>
                    <div class="next-bubble-body">${statusBadge(job.status)} ${escapeHtml(job.result_summary || job.error || '')}</div>
                  </div>
                `;
              });
          }

          function eventBubbles(record) {
            return (state.events || [])
              .filter((event) => event.tracked_pr_key === record.key)
              .slice(0, 6)
              .map((event) => `
                <div class="next-bubble ${event.level === 'error' ? 'warning' : ''}">
                  <div class="next-bubble-head"><span>${escapeHtml(event.event_type || 'event')}</span><span>${escapeHtml(event.created_at_label || '')}</span></div>
                  <div class="next-bubble-body">${escapeHtml(event.message || '')}</div>
                </div>
              `);
          }

          function renderConversation() {
            const pane = document.getElementById('conversation-pane');
            if (!pane) return;
            const record = selectedRecord();
            if (!record) {
              pane.innerHTML = '<div class="next-empty">No PR selected</div>';
              return;
            }
            const history = [
              ...liveActivityBubbles(record),
              ...jobBubbles(record),
              ...eventBubbles(record),
            ];
            const stream = history.length ? history.join('') : '<div class="next-bubble"><div class="next-bubble-body">No activity yet.</div></div>';
            pane.innerHTML = `
              <div class="next-record-head" data-record-key="${escapeHtml(record.key)}">
                <div class="next-title-row">
                  <div>
                    <h2><a href="${escapeHtml(record.pr_url)}">${escapeHtml(record.repo_name)} #${escapeHtml(record.pr_number)}</a> ${escapeHtml(record.pr_title)}</h2>
                    <div class="small">${escapeHtml(record.detail_text || '')}</div>
                  </div>
                  <div class="next-actions">${statusBadge(record.status)}${statusBadge(record.run_status)}</div>
                </div>
                <div class="next-meta-grid">
                  ${metaItem('Branch', record.branch, true)}
                  ${metaItem('Thread', `${record.thread?.short_id || ''} ${record.thread?.summary || ''}`)}
                  ${metaItem('Worktree', record.worktree_path, true)}
                  ${metaItem('Last poll', record.last_polled_label || '')}
                </div>
                ${compactActionButtons(record)}
                ${threadTools(record)}
              </div>
              <div class="next-history">
                <div class="next-stream">${stream}</div>
              </div>
              <div class="next-composer" data-record-key="${escapeHtml(record.key)}">
                <form id="steer-form">
                  <label class="next-hidden" for="steer-message">Steering message</label>
                  <textarea id="steer-message" name="message" placeholder="Add steering for ${escapeHtml(record.repo_name)} #${escapeHtml(record.pr_number)}"></textarea>
                  <button type="submit" class="primary">Queue message</button>
                </form>
              </div>
            `;
          }

          function renderDashboard() {
            ensureSelection();
            renderTabs();
            renderConversation();
            updateUrl();
          }

          async function loadDashboard(options = {}) {
            if (state.loading) return;
            state.loading = true;
            const params = new URLSearchParams(state.filters);
            try {
              const response = await fetch(`${DASHBOARD_API_URL}?${params.toString()}`, { headers: { Accept: 'application/json' } });
              const data = await response.json();
              if (!response.ok) throw new Error(data.message || 'Failed to load dashboard.');
              state.filters = data.filters;
              state.records = data.records || [];
              state.jobs = data.jobs || [];
              state.events = data.events || [];
              renderDashboard();
              if (!options.preserveFlash) showFlash('');
              state.refreshIntervalSeconds = nextRefreshInterval(state.records);
              state.secondsRemaining = state.refreshIntervalSeconds;
            } catch (error) {
              showFlash(error.message || 'Failed to load dashboard.', 'error');
            } finally {
              state.loading = false;
              updateRefreshControls();
            }
          }

          function markRecordPending(key, label) {
            document.querySelectorAll('[data-record-key]').forEach((container) => {
              if (container.dataset.recordKey !== key) return;
              container.querySelectorAll('button').forEach((button) => {
                if (button.closest('#steer-form')) return;
                button.disabled = true;
              });
            });
            const record = (state.records || []).find((item) => item.key === key);
            if (record) {
              record.run_status = 'queued';
              record.run_summary = label;
              record.run_summary_line = label;
              record.detail_text = record.detail_text ? `${record.detail_text} | pending: ${label}` : `pending: ${label}`;
            }
            renderDashboard();
          }

          async function postAction(path, params) {
            const response = await fetch(path, {
              method: 'POST',
              headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8', Accept: 'application/json' },
              body: new URLSearchParams(params).toString(),
            });
            const data = await response.json();
            if (!response.ok || !data.ok) throw new Error(data.message || 'Request failed.');
            showFlash(data.message || 'Queued action.');
            await loadDashboard({ preserveFlash: true });
          }

          document.addEventListener('DOMContentLoaded', () => {
            const filtersForm = document.getElementById('dashboard-filters');
            const refreshToggle = document.getElementById('refresh-toggle');
            const refreshNow = document.getElementById('refresh-now');

            if (filtersForm) {
              filtersForm.addEventListener('submit', (event) => {
                event.preventDefault();
                state.filters.scope = filtersForm.elements.scope.value;
                state.filters.status = filtersForm.elements.status.value;
                state.filters.sort = filtersForm.elements.sort.value;
                state.selectedKey = null;
                loadDashboard();
              });
            }

            if (refreshToggle) {
              refreshToggle.addEventListener('click', () => {
                state.refreshPaused = !state.refreshPaused;
                sessionStorage.setItem(REFRESH_PAUSE_KEY, state.refreshPaused ? '1' : '0');
                state.secondsRemaining = state.refreshIntervalSeconds;
                updateRefreshControls();
              });
            }

            if (refreshNow) refreshNow.addEventListener('click', () => loadDashboard());

            document.body.addEventListener('click', async (event) => {
              const selected = event.target.closest('[data-select-key]');
              if (selected) {
                state.selectedKey = selected.dataset.selectKey;
                renderDashboard();
                return;
              }
              const button = event.target.closest('button[data-action]');
              if (!button) return;
              const action = button.dataset.action;
              try {
                if (action === 'poll-all') {
                  await postAction(`${ACTION_API_BASE}/poll-all`, {});
                  return;
                }
                const key = button.dataset.key || selectedRecord()?.key;
                if (!key) return;
                if (action === 'set-thread') {
                  const wrapper = button.closest('[data-record-key]');
                  const input = wrapper ? wrapper.querySelector('[data-role="thread-id-input"]') : null;
                  markRecordPending(key, 'thread update queued');
                  await postAction(`${ACTION_API_BASE}/retarget-thread`, { key, thread_id: input ? input.value.trim() : '' });
                  return;
                }
                if (action === 'latest-thread') {
                  markRecordPending(key, 'latest thread queued');
                  await postAction(`${ACTION_API_BASE}/retarget-thread`, { key });
                  return;
                }
                if (action === 'fresh-thread') {
                  markRecordPending(key, 'fresh thread queued');
                  await postAction(`${ACTION_API_BASE}/retarget-thread`, { key, thread_id: NEW_THREAD_SENTINEL });
                  return;
                }
                const pendingLabel = action === 'stop-run' ? 'hard stop requested' : `${action.replace(/-/g, ' ')} queued`;
                markRecordPending(key, pendingLabel);
                await postAction(`${ACTION_API_BASE}/${action}`, { key });
              } catch (error) {
                showFlash(error.message || 'Request failed.', 'error');
                await loadDashboard({ preserveFlash: true });
              }
            });

            document.body.addEventListener('submit', async (event) => {
              if (event.target.id !== 'steer-form') return;
              event.preventDefault();
              const record = selectedRecord();
              const input = document.getElementById('steer-message');
              const message = input ? input.value.trim() : '';
              if (!record || !message) {
                showFlash('Enter a steering message.', 'error');
                return;
              }
              try {
                if (input) input.value = '';
                await postAction(`${ACTION_API_BASE}/steer`, { key: record.key, message });
              } catch (error) {
                showFlash(error.message || 'Request failed.', 'error');
                await loadDashboard({ preserveFlash: true });
              }
            });

            document.body.addEventListener('keydown', (event) => {
              if ((event.metaKey || event.ctrlKey) && event.key === 'Enter' && event.target.id === 'steer-message') {
                event.preventDefault();
                const form = document.getElementById('steer-form');
                if (form) form.requestSubmit();
              }
            });

            updateRefreshControls();
            loadDashboard();
            window.setInterval(() => {
              if (state.refreshPaused || state.loading) {
                updateRefreshControls();
                return;
              }
              state.secondsRemaining -= 1;
              if (state.secondsRemaining <= 0) {
                loadDashboard();
                return;
              }
              updateRefreshControls();
            }, 1000);
          });
        </script>
"""
