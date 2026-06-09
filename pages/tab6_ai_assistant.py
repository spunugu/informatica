"""
ETL Automator — Tab 6: Multi-AI Assistant
Built by Srinivas Punugu

Supports 3 AI providers:
- Claude (Anthropic) — claude-haiku-4-5 (fast, cheap)
- ChatGPT (OpenAI)   — gpt-4o-mini (free tier friendly)
- Gemini (Google)    — gemini-1.5-flash (free tier)

User provides their own API key for whichever they want to use.
Keys stored only in session state — never persisted.
"""

import streamlit as st
import requests
import json
import os
from datetime import datetime


# ─── AI Provider configs ───────────────────────────────────────────────────────

AI_PROVIDERS = {
    "claude": {
        "name":    "Claude",
        "company": "Anthropic",
        "icon":    "🤖",
        "model":   "claude-haiku-4-5-20251001",
        "color":   "#6366f1",
        "free":    "No free tier — cheapest model is Haiku (~$0.001/1K tokens)",
        "get_key": "https://console.anthropic.com/api-keys",
        "placeholder": "sk-ant-api03-...",
    },
    "chatgpt": {
        "name":    "ChatGPT",
        "company": "OpenAI",
        "icon":    "🟢",
        "model":   "gpt-4o-mini",
        "color":   "#10a37f",
        "free":    "Free $5 credit on signup — gpt-4o-mini is very cheap",
        "get_key": "https://platform.openai.com/api-keys",
        "placeholder": "sk-proj-...",
    },
    "gemini": {
        "name":    "Gemini",
        "company": "Google",
        "icon":    "💎",
        "model":   "gemini-1.5-flash",
        "color":   "#4285f4",
        "free":    "✅ FREE tier available — 15 requests/min, no credit card needed",
        "get_key": "https://aistudio.google.com/app/apikey",
        "placeholder": "AIza...",
    },
}


# ─── System prompt ─────────────────────────────────────────────────────────────

def _build_system_prompt() -> str:
    workflow    = st.session_state.get("selected_workflow", "No workflow loaded yet")
    folder      = st.session_state.get("selected_folder", "")
    sessions    = st.session_state.get("curr_sessions", [])
    worklets    = st.session_state.get("curr_worklets", [])
    mappings    = st.session_state.get("curr_mappings", [])
    sources     = st.session_state.get("curr_sources", [])
    targets     = st.session_state.get("curr_targets", [])
    lookups     = st.session_state.get("curr_lookups", [])
    complexity  = st.session_state.get("complexity", "Unknown")
    row_count   = st.session_state.get("row_count", 0)
    sqls        = st.session_state.get("generated_sqls", {})
    dag         = st.session_state.get("generated_dag", "")
    val_logic   = st.session_state.get("logic_results", {})
    val_rows    = st.session_state.get("rowcount_results", {})
    val_schema  = st.session_state.get("schema_results", {})
    project     = st.session_state.get("bq_project", "your-gcp-project")
    dataset_map = st.session_state.get("dataset_map", {})

    sql_context = ""
    if sqls:
        for session, sql in list(sqls.items())[:1]:
            sql_context += f"\n\n--- SQL for {session} (first 600 chars) ---\n{sql[:600]}"

    val_context = ""
    if val_logic:
        val_context += f"\nLogic: {val_logic.get('passed',0)} passed, {val_logic.get('failed',0)} failed"
    if val_rows:
        val_context += f"\nRow count delta: {val_rows.get('delta_pct',0):.4f}%"
    if val_schema:
        val_context += f"\nSchema: {val_schema.get('matched_columns',0)} matched, {val_schema.get('type_mismatches',0)} mismatches"

    return f"""You are an expert ETL migration AI assistant inside ETL Automator, built by Srinivas Punugu.

You specialize in Informatica PowerCenter → Google BigQuery + Apache Airflow migrations.

## Current Workflow Context:
- Workflow   : {workflow}
- Folder     : {folder}
- Complexity : {complexity}
- Sessions   : {', '.join(sessions) if sessions else 'None loaded'}
- Sources    : {', '.join(sources) if sources else 'None'}
- Targets    : {', '.join(targets) if targets else 'None'}
- Lookups    : {', '.join(lookups) if lookups else 'None'}
- Row count  : {row_count:,}
- GCP Project: {project}
- Datasets   : {json.dumps(dataset_map)}

## Generated SQL:{sql_context if sql_context else ' Not generated yet'}
## Validation:{val_context if val_context else ' Not run yet'}

Be concise and technical. Use ```sql and ```python code blocks.
Reference actual workflow/session/table names from context.
"""


# ─── API callers ───────────────────────────────────────────────────────────────

def _call_claude(messages: list, system_prompt: str, api_key: str) -> str:
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1500,
                "system": system_prompt,
                "messages": messages,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
        elif resp.status_code == 401:
            return "❌ Invalid Claude API key. Get yours at console.anthropic.com/api-keys"
        return f"❌ Claude error {resp.status_code}: {resp.text[:150]}"
    except Exception as e:
        return f"❌ Claude error: {str(e)}"


def _call_chatgpt(messages: list, system_prompt: str, api_key: str) -> str:
    try:
        gpt_messages = [{"role": "system", "content": system_prompt}]
        for m in messages:
            gpt_messages.append({"role": m["role"], "content": m["content"]})

        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": gpt_messages,
                "max_tokens": 1500,
                "temperature": 0.3,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"]
        elif resp.status_code == 401:
            return "❌ Invalid OpenAI API key. Get yours at platform.openai.com/api-keys"
        elif resp.status_code == 429:
            return "❌ Rate limit hit. Wait a moment and try again."
        return f"❌ ChatGPT error {resp.status_code}: {resp.text[:150]}"
    except Exception as e:
        return f"❌ ChatGPT error: {str(e)}"


def _call_gemini(messages: list, system_prompt: str, api_key: str) -> str:
    try:
        # Build Gemini contents
        contents = []
        # Add system as first user message (Gemini doesn't have system role)
        contents.append({
            "role": "user",
            "parts": [{"text": f"[System instructions]: {system_prompt}\n\nAcknowledge and wait for my question."}]
        })
        contents.append({
            "role": "model",
            "parts": [{"text": "Understood. I'm ready to help with your ETL migration questions."}]
        })
        for m in messages:
            role = "model" if m["role"] == "assistant" else "user"
            contents.append({"role": role, "parts": [{"text": m["content"]}]})

        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": contents,
                "generationConfig": {
                    "maxOutputTokens": 1500,
                    "temperature": 0.3,
                },
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        elif resp.status_code == 400:
            return "❌ Invalid Gemini API key or request. Get your key at aistudio.google.com/app/apikey"
        elif resp.status_code == 429:
            return "❌ Gemini rate limit hit. Free tier = 15 req/min. Wait a moment."
        return f"❌ Gemini error {resp.status_code}: {resp.text[:150]}"
    except Exception as e:
        return f"❌ Gemini error: {str(e)}"


def _call_ai(messages: list, system_prompt: str, provider: str, api_key: str) -> str:
    """Route to the right AI provider."""
    if provider == "claude":
        return _call_claude(messages, system_prompt, api_key)
    elif provider == "chatgpt":
        return _call_chatgpt(messages, system_prompt, api_key)
    elif provider == "gemini":
        return _call_gemini(messages, system_prompt, api_key)
    return "❌ Unknown provider"


# ─── Suggested questions ───────────────────────────────────────────────────────

def _get_suggested_questions() -> list:
    workflow  = st.session_state.get("selected_workflow", "")
    sessions  = st.session_state.get("curr_sessions", [])
    sqls      = st.session_state.get("generated_sqls", {})
    val_logic = st.session_state.get("logic_results", {})

    questions = [
        "What does this workflow do and what is the migration complexity?",
        "Explain the MERGE statement in the generated BigQuery SQL",
        "How do I convert Informatica IIF expressions to BigQuery SQL?",
        "What is the BigQuery equivalent of an Informatica Lookup transformation?",
        "How should I partition and cluster the target BigQuery table?",
        "What are the Airflow best practices for this DAG?",
        "How do I handle SCD Type 2 in BigQuery?",
        "How do I replace pmcmd shell scripts with Cloud Functions?",
        "What BigQuery optimizations should I apply to this workflow?",
        "Explain the difference between DD_INSERT and DD_UPDATE in Informatica",
    ]

    if sessions:
        questions.insert(0, f"Explain what session `{sessions[0]}` does step by step")
    if sqls:
        questions.insert(1, "Why does the generated SQL use TEMP tables instead of CTEs?")
    if val_logic and val_logic.get("failed", 0) > 0:
        questions.insert(0, "My validation failed — what are the likely causes and fixes?")

    return questions[:6]


# ─── Main render ──────────────────────────────────────────────────────────────

def render():
    st.markdown("""
    <div class="tab-header">
        <h2>🤖 AI Assistant</h2>
        <p>Ask Claude, ChatGPT or Gemini about your ETL migration. Each knows your full workflow context.</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Provider selector ─────────────────────────────────────────────────────
    st.markdown("#### 🔌 Choose AI Provider")

    provider_cols = st.columns(3)
    for i, (key, cfg) in enumerate(AI_PROVIDERS.items()):
        with provider_cols[i]:
            selected = st.session_state.get("ai_provider", "gemini") == key
            border   = cfg["color"] if selected else "#334155"
            st.markdown(f"""
            <div style="background:#1e293b;border:2px solid {border};border-radius:10px;
                        padding:14px;text-align:center;margin-bottom:8px;">
                <div style="font-size:28px;">{cfg['icon']}</div>
                <div style="font-size:14px;font-weight:700;color:#f1f5f9;margin-top:4px;">
                    {cfg['name']}
                </div>
                <div style="font-size:11px;color:#64748b;">{cfg['company']}</div>
                <div style="font-size:10px;color:{'#22c55e' if 'FREE' in cfg['free'] else '#f59e0b'};
                            margin-top:6px;">{cfg['free'][:45]}</div>
            </div>
            """, unsafe_allow_html=True)
            if st.button(
                f"{'✅ Selected' if selected else f'Use {cfg[\"name\"]}'}",
                key=f"sel_{key}",
                use_container_width=True,
                type="primary" if selected else "secondary"
            ):
                st.session_state.ai_provider = key
                st.session_state.chat_messages = []
                st.rerun()

    # Current provider
    provider = st.session_state.get("ai_provider", "gemini")
    cfg      = AI_PROVIDERS[provider]

    st.markdown("---")

    # ── API Key input ─────────────────────────────────────────────────────────
    st.markdown(f"#### 🔑 {cfg['name']} API Key")

    key_col1, key_col2 = st.columns([3, 1])
    with key_col1:
        # Check Streamlit secrets first
        secret_key = ""
        try:
            secret_map = {"claude": "ANTHROPIC_API_KEY", "chatgpt": "OPENAI_API_KEY", "gemini": "GEMINI_API_KEY"}
            secret_key = st.secrets.get(secret_map[provider], "")
        except Exception:
            pass

        if secret_key:
            st.success(f"✅ {cfg['name']} API key loaded from Streamlit secrets")
            st.session_state[f"api_key_{provider}"] = secret_key
        else:
            api_key_input = st.text_input(
                f"Paste your {cfg['name']} API key",
                type="password",
                value=st.session_state.get(f"api_key_{provider}", ""),
                placeholder=cfg["placeholder"],
                help=f"Your key is stored only in this session — never saved or sent anywhere except {cfg['company']}'s API",
                key=f"key_input_{provider}"
            )
            if api_key_input:
                st.session_state[f"api_key_{provider}"] = api_key_input

    with key_col2:
        st.markdown(f"""
        <div style="background:#1e293b;border:1px solid #334155;border-radius:8px;
                    padding:12px;text-align:center;margin-top:4px;">
            <div style="font-size:11px;color:#64748b;margin-bottom:6px;">Get API Key</div>
            <a href="{cfg['get_key']}" target="_blank"
               style="color:{cfg['color']};font-size:12px;font-weight:600;text-decoration:none;">
                🔗 {cfg['company']} Console →
            </a>
        </div>
        """, unsafe_allow_html=True)

    # Free tier note for Gemini
    if provider == "gemini":
        st.info("💎 **Gemini is FREE** — Go to aistudio.google.com/app/apikey, sign in with Google, click 'Create API key'. No credit card needed!", icon="✅")
    elif provider == "chatgpt":
        st.info("🟢 **ChatGPT** — Free $5 credit on signup. gpt-4o-mini costs ~$0.15 per 1M tokens (very cheap). Get key at platform.openai.com/api-keys", icon="ℹ️")
    elif provider == "claude":
        st.info("🤖 **Claude Haiku** — Fastest & cheapest Claude model. Get key at console.anthropic.com/api-keys", icon="ℹ️")

    api_key = st.session_state.get(f"api_key_{provider}", "")

    st.markdown("---")

    # ── Context status ────────────────────────────────────────────────────────
    workflow   = st.session_state.get("selected_workflow", "")
    sqls       = st.session_state.get("generated_sqls", {})
    val_done   = len(st.session_state.get("validation_steps", set())) >= 3
    sessions   = st.session_state.get("curr_sessions", [])
    complexity = st.session_state.get("complexity", "")

    ctx_items = []
    ctx_items.append(f"{'✅' if workflow else '⬜'} Workflow: `{workflow}`" if workflow else "⬜ No workflow loaded")
    ctx_items.append(f"{'✅' if sqls else '⬜'} SQL {'generated' if sqls else 'not generated'}")
    ctx_items.append(f"{'✅' if val_done else '⬜'} Validation {'done' if val_done else 'pending'}")
    ctx_items.append(f"{'✅' if api_key else '❌'} {cfg['name']} key {'ready' if api_key else 'missing'}")

    st.markdown(
        "<div style='background:#1e293b;border:1px solid #334155;border-radius:8px;"
        "padding:8px 16px;margin-bottom:12px;display:flex;gap:24px;flex-wrap:wrap;'>"
        + "".join([f"<span style='font-size:12px;color:#94a3b8;'>{i}</span>" for i in ctx_items])
        + "</div>",
        unsafe_allow_html=True
    )

    # ── Init chat ─────────────────────────────────────────────────────────────
    current_greeting_has_workflow = any(
        workflow in m["content"] for m in st.session_state.get("chat_messages", [])
        if m["role"] == "assistant"
    ) if workflow else True

    if "chat_messages" not in st.session_state or not current_greeting_has_workflow:
        if workflow:
            greeting = (
                f"{cfg['icon']} Hi! I'm **{cfg['name']}**, your ETL migration assistant.\n\n"
                f"I can see you're working on **`{workflow}`** "
                f"({'**' + complexity + '** complexity, ' if complexity else ''}"
                f"**{len(sessions)} sessions**"
                f"{', ✅ SQL generated' if sqls else ''}"
                f"{', ✅ Validation done' if val_done else ''}).\n\n"
                f"Ask me anything about your migration!"
            )
        else:
            greeting = (
                f"{cfg['icon']} Hi! I'm **{cfg['name']}**, your ETL migration assistant.\n\n"
                f"I specialize in **Informatica PowerCenter → BigQuery + Airflow** migrations.\n\n"
                f"Load a workflow in **Tab 1** first for full context, or ask me anything!"
            )
        st.session_state.chat_messages = [{"role": "assistant", "content": greeting}]

    # ── Suggested questions ───────────────────────────────────────────────────
    with st.expander("💡 Suggested Questions", expanded=len(st.session_state.chat_messages) <= 1):
        suggestions = _get_suggested_questions()
        cols = st.columns(2)
        for i, q in enumerate(suggestions):
            with cols[i % 2]:
                if st.button(q, key=f"sugg_{provider}_{i}", use_container_width=True):
                    st.session_state.pending_question = q
                    st.rerun()

    st.markdown("---")

    # ── Chat history ──────────────────────────────────────────────────────────
    for msg in st.session_state.chat_messages:
        if msg["role"] == "user":
            st.markdown(f"""
            <div style="display:flex;justify-content:flex-end;margin:8px 0;">
                <div style="background:#6366f1;color:white;border-radius:12px 12px 2px 12px;
                            padding:10px 16px;max-width:75%;font-size:14px;line-height:1.5;">
                    {msg["content"]}
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            with st.chat_message("assistant", avatar=cfg["icon"]):
                st.markdown(msg["content"])

    # ── Handle pending question ───────────────────────────────────────────────
    if "pending_question" in st.session_state:
        question = st.session_state.pop("pending_question")
        _handle_message(question, provider, api_key)
        st.rerun()

    # ── Chat input ────────────────────────────────────────────────────────────
    st.markdown("---")
    col_input, col_clear = st.columns([5, 1])
    with col_input:
        user_input = st.chat_input(
            f"Ask {cfg['name']} about your ETL migration...",
            key=f"chat_input_{provider}"
        )
    with col_clear:
        if st.button("🗑️ Clear", use_container_width=True, key=f"clear_{provider}"):
            st.session_state.chat_messages = []
            st.rerun()

    if user_input:
        _handle_message(user_input, provider, api_key)
        st.rerun()

    # ── Quick actions ─────────────────────────────────────────────────────────
    st.markdown("#### ⚡ Quick Actions")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("📋 Summarize Workflow", use_container_width=True, key=f"qa1_{provider}"):
            wf = st.session_state.get("selected_workflow", "the workflow")
            sessions_list = st.session_state.get("curr_sessions", [])
            sources_list  = st.session_state.get("curr_sources", [])
            targets_list  = st.session_state.get("curr_targets", [])
            st.session_state.pending_question = (
                f"Give me a concise technical summary of `{wf}` — "
                f"what it does, its {len(sessions_list)} sessions, "
                f"sources ({', '.join(sources_list[:2])}), "
                f"targets ({', '.join(targets_list[:2])}), and migration complexity."
            )
            st.rerun()

    with col2:
        if st.button("🔍 Explain SQL", use_container_width=True, key=f"qa2_{provider}"):
            sqls_local = st.session_state.get("generated_sqls", {})
            if sqls_local:
                first = list(sqls_local.keys())[0]
                st.session_state.pending_question = f"Explain the generated BigQuery SQL for `{first}` step by step."
            else:
                st.session_state.pending_question = "How does ETL Automator convert Informatica XML to BigQuery SQL?"
            st.rerun()

    with col3:
        if st.button("⚠️ Fix Validation", use_container_width=True, key=f"qa3_{provider}"):
            val = st.session_state.get("logic_results", {})
            failed = [r for r in val.get("results", []) if r.get("status") == "FAIL"]
            if failed:
                names = ", ".join([r["session"] for r in failed[:3]])
                st.session_state.pending_question = f"Validation failed for: {names}. What are the causes and fixes?"
            else:
                st.session_state.pending_question = "What are common validation failures in Informatica → BigQuery migrations?"
            st.rerun()

    with col4:
        if st.button("🚀 Deploy Checklist", use_container_width=True, key=f"qa4_{provider}"):
            wf = st.session_state.get("selected_workflow", "this workflow")
            st.session_state.pending_question = (
                f"Give me a production deployment checklist for `{wf}` — "
                f"what must I verify before going live on BigQuery + Airflow?"
            )
            st.rerun()


# ─── Message handler ───────────────────────────────────────────────────────────

def _handle_message(user_text: str, provider: str, api_key: str):
    st.session_state.chat_messages.append({"role": "user", "content": user_text})

    if not api_key:
        cfg = AI_PROVIDERS[provider]
        st.session_state.chat_messages.append({
            "role": "assistant",
            "content": (
                f"⚠️ **No {cfg['name']} API key provided.**\n\n"
                f"Please paste your API key in the field above.\n\n"
                f"👉 Get it free at: {cfg['get_key']}\n\n"
                f"{'💡 **Gemini is completely free** — no credit card needed!' if provider == 'gemini' else ''}"
            )
        })
        return

    api_messages = [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.chat_messages
        if m["role"] in ("user", "assistant")
    ][-10:]

    system_prompt = _build_system_prompt()

    with st.spinner(f"{AI_PROVIDERS[provider]['icon']} {AI_PROVIDERS[provider]['name']} is thinking..."):
        response = _call_ai(api_messages, system_prompt, provider, api_key)

    st.session_state.chat_messages.append({"role": "assistant", "content": response})
