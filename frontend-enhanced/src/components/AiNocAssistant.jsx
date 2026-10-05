import { useState, useEffect, useRef } from 'react';
import { sendAiChatMessage, getAiPlugins } from '../api/client';
import './ai-noc-assistant.css';

const STORAGE_KEY_SESSIONS = 'gnc_noc_ai_sessions_v2';
const STORAGE_KEY_ACTIVE_ID = 'gnc_noc_ai_active_id_v2';
const STORAGE_KEY_STATE = 'gnc_noc_ai_widget_state_v1';

const QUICK_PROMPTS = [
  { label: 'Network Summary', text: 'Give me the overall Milano network activity summary and current health status.' },
  { label: 'Active Alerts', text: 'What are the recent rule-based anomaly alerts across the network?' },
  { label: 'Top Grids', text: 'Which grids are experiencing the highest network activity today?' },
  { label: 'Investigate Grid 5000', text: 'Investigate grid 5000: check its recent activity, ML anomaly prediction, and alerts.' },
];

function createDefaultSession() {
  const id = `session_${Date.now()}`;
  return {
    id,
    title: 'New Investigation',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    messages: [
      {
        id: 'msg_welcome',
        role: 'assistant',
        content: `**Milano Telecom AI NOC Assistant Ready**\n\nI am connected to the Milano Network Analytics engine with live access to:\n- Citywide traffic metrics & peak hours\n- ML-based drop anomaly predictions (XGBoost)\n- Rule-based alerts & traffic hotspot detectors\n- Per-grid timeseries and feature distributions\n\nAsk me to investigate any grid (e.g. *grid 5000*), diagnose anomalies, or summarize citywide traffic.`,
        timestamp: new Date().toISOString(),
      },
    ],
  };
}

function formatTime(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatDate(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  const now = new Date();
  const isToday = d.toDateString() === now.toDateString();
  if (isToday) {
    return `Today at ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
  }
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

// Simple parser for NOC markdown output to render sections, code blocks, lists and bold
function FormattedMessage({ text }) {
  if (!text) return null;

  // Split by line blocks
  const lines = text.split('\n');
  const renderedElements = [];
  let inCodeBlock = false;
  let codeBuffer = [];

  lines.forEach((line, index) => {
    // Code block toggle
    if (line.trim().startsWith('```')) {
      if (inCodeBlock) {
        renderedElements.push(
          <pre key={`code_${index}`} className="noc-code-block">
            <code>{codeBuffer.join('\n')}</code>
          </pre>
        );
        codeBuffer = [];
        inCodeBlock = false;
      } else {
        inCodeBlock = true;
      }
      return;
    }

    if (inCodeBlock) {
      codeBuffer.push(line);
      return;
    }

    // Severity badges
    const upper = line.trim().toUpperCase();
    if (upper.startsWith('SEVERITY:') || upper === 'SEVERITY') {
      let severityClass = 'is-info';
      if (upper.includes('CRITICAL') || upper.includes('HIGH')) severityClass = 'is-critical';
      else if (upper.includes('WARN') || upper.includes('MEDIUM')) severityClass = 'is-warn';
      else if (upper.includes('LOW') || upper.includes('NORMAL') || upper.includes('OK')) severityClass = 'is-ok';

      renderedElements.push(
        <div key={`sev_${index}`} className={`noc-section-header ${severityClass}`}>
          <span className="noc-section-tag">SEVERITY</span>
          <span className="noc-section-val">{line.replace(/^SEVERITY:?/i, '').trim()}</span>
        </div>
      );
      return;
    }

    // Structured headers
    if (upper.startsWith('EVIDENCE:') || upper === 'EVIDENCE' ||
        upper.startsWith('INTERPRETATION:') || upper === 'INTERPRETATION' ||
        upper.startsWith('NEXT CHECKS:') || upper === 'NEXT CHECKS') {
      renderedElements.push(
        <div key={`hdr_${index}`} className="noc-structured-header">
          {line.trim()}
        </div>
      );
      return;
    }

    // Markdown bullet point
    if (line.trim().startsWith('- ') || line.trim().startsWith('* ')) {
      const content = line.trim().substring(2);
      renderedElements.push(
        <div key={`bullet_${index}`} className="noc-bullet-item">
          <span className="noc-bullet-dot">›</span>
          <span>{renderInlineText(content)}</span>
        </div>
      );
      return;
    }

    // Empty lines as spacer
    if (!line.trim()) {
      renderedElements.push(<div key={`space_${index}`} className="noc-line-spacer" />);
      return;
    }

    // Standard paragraph with bold/code formatting
    renderedElements.push(
      <p key={`p_${index}`} className="noc-para">
        {renderInlineText(line)}
      </p>
    );
  });

  if (inCodeBlock && codeBuffer.length > 0) {
    renderedElements.push(
      <pre key="code_end" className="noc-code-block">
        <code>{codeBuffer.join('\n')}</code>
      </pre>
    );
  }

  return <div className="noc-formatted-content">{renderedElements}</div>;
}

function renderInlineText(text) {
  // Regex to split on **bold** and `code`
  const parts = text.split(/(\*\*.*?\*\*|`.*?`)/g);
  return parts.map((part, i) => {
    if (part.startsWith('**') && part.endsWith('**')) {
      return <strong key={i} className="noc-bold">{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith('`') && part.endsWith('`')) {
      return <code key={i} className="noc-inline-code">{part.slice(1, -1)}</code>;
    }
    return part;
  });
}

export default function AiNocAssistant() {
  const [isOpen, setIsOpen] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [showHistoryPanel, setShowHistoryPanel] = useState(false);
  const [sessions, setSessions] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY_SESSIONS);
      if (saved) {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length > 0) {
          return parsed;
        }
      }
    } catch (e) {
      console.warn('Failed to load saved chat sessions:', e);
    }
    return [createDefaultSession()];
  });

  const [activeSessionId, setActiveSessionId] = useState(() => {
    try {
      const savedId = localStorage.getItem(STORAGE_KEY_ACTIVE_ID);
      if (savedId) return savedId;
    } catch (e) {
      // ignore
    }
    return sessions[0]?.id || `session_${Date.now()}`;
  });

  const [inputMessage, setInputMessage] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [copiedMsgId, setCopiedMsgId] = useState(null);
  const [plugins, setPlugins] = useState([]);
  const [expandedTraces, setExpandedTraces] = useState({});

  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  // Load registered NOC plugins from backend
  useEffect(() => {
    getAiPlugins()
      .then((data) => {
        if (Array.isArray(data)) setPlugins(data);
      })
      .catch((err) => {
        console.warn('Failed to load NOC plugins:', err);
      });
  }, []);

  const toggleTrace = (msgId) => {
    setExpandedTraces((prev) => ({
      ...prev,
      [msgId]: !prev[msgId],
    }));
  };

  // Restore widget open/expanded state if user previously used it
  useEffect(() => {
    try {
      const savedState = localStorage.getItem(STORAGE_KEY_STATE);
      if (savedState) {
        const { open, expanded } = JSON.parse(savedState);
        if (typeof open === 'boolean') setIsOpen(open);
        if (typeof expanded === 'boolean') setIsExpanded(expanded);
      }
    } catch (e) {
      // ignore
    }
  }, []);

  // Persist sessions
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY_SESSIONS, JSON.stringify(sessions));
    } catch (e) {
      console.warn('Failed to persist chat sessions:', e);
    }
  }, [sessions]);

  // Persist active session ID
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY_ACTIVE_ID, activeSessionId);
    } catch (e) {
      // ignore
    }
  }, [activeSessionId]);

  // Persist open/expanded state
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY_STATE, JSON.stringify({ open: isOpen, expanded: isExpanded }));
    } catch (e) {
      // ignore
    }
  }, [isOpen, isExpanded]);

  // Auto scroll to bottom
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const activeSession = sessions.find((s) => s.id === activeSessionId) || sessions[0] || createDefaultSession();

  useEffect(() => {
    if (isOpen) {
      scrollToBottom();
      if (!showHistoryPanel) {
        inputRef.current?.focus();
      }
    }
  }, [isOpen, activeSession?.messages?.length, showHistoryPanel]);

  // Handler to start a new chat session
  const handleNewChat = () => {
    const newSession = {
      id: `session_${Date.now()}`,
      title: 'New Investigation',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      messages: [
        {
          id: `msg_${Date.now()}`,
          role: 'assistant',
          content: 'Started a new NOC investigation session. What network event or grid should we inspect?',
          timestamp: new Date().toISOString(),
        },
      ],
    };
    setSessions((prev) => [newSession, ...prev]);
    setActiveSessionId(newSession.id);
    setShowHistoryPanel(false);
    setInputMessage('');
    setTimeout(() => inputRef.current?.focus(), 50);
  };

  // Handler to select a session
  const handleSelectSession = (id) => {
    setActiveSessionId(id);
    setShowHistoryPanel(false);
    setTimeout(() => inputRef.current?.focus(), 50);
  };

  // Handler to delete a specific session
  const handleDeleteSession = (e, id) => {
    e.stopPropagation();
    if (sessions.length <= 1) {
      // Reset to a clean default session
      const fresh = createDefaultSession();
      setSessions([fresh]);
      setActiveSessionId(fresh.id);
      return;
    }

    const nextSessions = sessions.filter((s) => s.id !== id);
    setSessions(nextSessions);
    if (activeSessionId === id) {
      setActiveSessionId(nextSessions[0].id);
    }
  };

  // Clear all sessions
  const handleClearAllSessions = () => {
    if (window.confirm('Clear all stored AI NOC chat sessions and history?')) {
      const fresh = createDefaultSession();
      setSessions([fresh]);
      setActiveSessionId(fresh.id);
      setShowHistoryPanel(false);
    }
  };

  // Send message
  const handleSendMessage = async (textToSend) => {
    const text = (textToSend || inputMessage).trim();
    if (!text || isLoading) return;

    setInputMessage('');

    const userMsgId = `msg_user_${Date.now()}`;
    const userMessageObj = {
      id: userMsgId,
      role: 'user',
      content: text,
      timestamp: new Date().toISOString(),
    };

    // Prepare history from active session messages
    const currentMessages = activeSession?.messages || [];
    const historyPayload = currentMessages
      .filter((m) => m.role === 'user' || m.role === 'assistant')
      .map((m) => ({ role: m.role, content: m.content }));

    // Auto title session from first real query if still default title
    const isFirstUserMsg = !currentMessages.some((m) => m.role === 'user');
    const updatedTitle = isFirstUserMsg
      ? text.length > 38 ? `${text.slice(0, 38)}...` : text
      : activeSession.title;

    // Optimistically update session with user message
    const updatedSessions = sessions.map((s) => {
      if (s.id === activeSessionId) {
        return {
          ...s,
          title: updatedTitle,
          updatedAt: new Date().toISOString(),
          messages: [...s.messages, userMessageObj],
        };
      }
      return s;
    });

    setSessions(updatedSessions);
    setIsLoading(true);

    try {
      const result = await sendAiChatMessage(text, historyPayload);
      const assistantMsgId = `msg_asst_${Date.now()}`;
      const assistantMessageObj = {
        id: assistantMsgId,
        role: 'assistant',
        content: result.response || 'No response returned from the NOC assistant.',
        timestamp: new Date().toISOString(),
        traces: result.traces || [],
        duration_ms: result.duration_ms || 0,
        active_plugins: result.active_plugins || [],
        compliance_passed: result.compliance_passed ?? true,
      };

      setSessions((prev) =>
        prev.map((s) => {
          if (s.id === activeSessionId) {
            return {
              ...s,
              updatedAt: new Date().toISOString(),
              messages: [...s.messages, assistantMessageObj],
            };
          }
          return s;
        })
      );
    } catch (err) {
      const errorMsg =
        err.response?.data?.detail ||
        err.message ||
        'Failed to reach the AI NOC backend service. Please check network connectivity.';

      const errorObj = {
        id: `msg_err_${Date.now()}`,
        role: 'assistant',
        content: `**NOC Alert / Communication Failure**\n\n${errorMsg}`,
        timestamp: new Date().toISOString(),
        isError: true,
      };

      setSessions((prev) =>
        prev.map((s) => {
          if (s.id === activeSessionId) {
            return {
              ...s,
              updatedAt: new Date().toISOString(),
              messages: [...s.messages, errorObj],
            };
          }
          return s;
        })
      );
    } finally {
      setIsLoading(false);
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  useEffect(() => {
    const handleAskAi = (e) => {
      const prompt = e.detail?.prompt;
      if (prompt) {
        setIsOpen(true);
        setShowHistoryPanel(false);
        if (e.detail?.autoSend) {
          handleSendMessage(prompt);
        } else {
          setInputMessage(prompt);
          setTimeout(() => inputRef.current?.focus(), 50);
        }
      }
    };
    window.addEventListener('gnc-ask-ai', handleAskAi);
    return () => window.removeEventListener('gnc-ask-ai', handleAskAi);
  }, [sessions, activeSessionId, isLoading]);

  const copyToClipboard = (id, content) => {
    navigator.clipboard.writeText(content);
    setCopiedMsgId(id);
    setTimeout(() => setCopiedMsgId(null), 1800);
  };

  // Unread badge count indicator if closed
  const totalUserMessages = activeSession?.messages?.filter((m) => m.role === 'user')?.length || 0;

  return (
    <div className={`noc-assistant-container ${isOpen ? 'is-open' : 'is-collapsed'} ${isExpanded ? 'is-expanded' : ''}`}>
      {/* Floating Trigger Button (when collapsed) */}
      {!isOpen && (
        <button
          type="button"
          className="noc-assistant-trigger"
          onClick={() => setIsOpen(true)}
          title="Open Milano AI NOC Assistant"
          aria-label="Open Milano AI NOC Assistant"
        >
          <div className="noc-trigger-pulse-ring" />
          <div className="noc-trigger-icon-wrap">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z" />
              <path d="M19 10v2a7 7 0 0 1-14 0v-2" />
              <line x1="12" x2="12" y1="19" y2="22" />
              <line x1="8" x2="16" y1="22" y2="22" />
            </svg>
            <span className="noc-status-indicator" />
          </div>
          <div className="noc-trigger-label">
            <span className="noc-trigger-main">AI NOC Assistant</span>
            <span className="noc-trigger-sub">Live Copilot</span>
          </div>
          {totalUserMessages > 0 && (
            <span className="noc-trigger-badge" title="Active conversation turns">
              {totalUserMessages}
            </span>
          )}
        </button>
      )}

      {/* Floating Expandable Assistant Window */}
      {isOpen && (
        <div className="noc-assistant-window" role="dialog" aria-label="AI NOC Assistant Panel">
          {/* Header Bar */}
          <div className="noc-window-header">
            <div className="noc-header-branding">
              <div className="noc-header-title-block">
                <div className="noc-header-title">
                  AI NOC Assistant
                </div>
                <div className="noc-header-status">
                  <span className="noc-live-dot" />
                  <span>Agent Online • 7 Analytics Tools Active</span>
                </div>
              </div>
            </div>

            {/* Action buttons */}
            <div className="noc-header-actions">
              {/* History / Previous Chats Drawer Toggle */}
              <button
                type="button"
                className={`noc-action-btn ${showHistoryPanel ? 'is-active' : ''}`}
                onClick={() => setShowHistoryPanel((prev) => !prev)}
                title={showHistoryPanel ? 'Return to active chat' : 'View previous chat sessions'}
                aria-label="Chat history"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 8v4l3 3" />
                  <circle cx="12" cy="12" r="9" />
                </svg>
                <span className="noc-btn-label">Chats ({sessions.length})</span>
              </button>

              {/* New Chat */}
              <button
                type="button"
                className="noc-action-btn"
                onClick={handleNewChat}
                title="Start a new investigation session"
                aria-label="New chat"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 5v14M5 12h14" />
                </svg>
                <span className="noc-btn-label">New</span>
              </button>

              {/* Expand / Minimize Window Size */}
              <button
                type="button"
                className="noc-action-btn noc-expand-btn"
                onClick={() => setIsExpanded((prev) => !prev)}
                title={isExpanded ? 'Restore compact window' : 'Expand window to wide view'}
                aria-label={isExpanded ? 'Restore size' : 'Expand window'}
              >
                {isExpanded ? (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="4 14 10 14 10 20" />
                    <polyline points="20 10 14 10 14 4" />
                    <line x1="14" y1="10" x2="21" y2="3" />
                    <line x1="3" y1="21" x2="10" y2="14" />
                  </svg>
                ) : (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="15 3 21 3 21 9" />
                    <polyline points="9 21 3 21 3 15" />
                    <line x1="21" y1="3" x2="14" y2="10" />
                    <line x1="3" y1="21" x2="10" y2="14" />
                  </svg>
                )}
              </button>

              {/* Close / Minimize to floating badge */}
              <button
                type="button"
                className="noc-action-btn noc-close-btn"
                onClick={() => setIsOpen(false)}
                title="Minimize assistant"
                aria-label="Close assistant"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18" />
                  <line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
          </div>

          {/* Body Section */}
          <div className="noc-window-body">
            {/* Previous Chats / Sessions Drawer */}
            {showHistoryPanel ? (
              <div className="noc-sessions-view">
                <div className="noc-sessions-header">
                  <div>
                    <h3 className="noc-sessions-title">Stored NOC Investigations</h3>
                    <p className="noc-sessions-desc">Switch between previous chat sessions or start a new diagnosis.</p>
                  </div>
                  <button
                    type="button"
                    className="noc-clear-all-btn"
                    onClick={handleClearAllSessions}
                    title="Clear all stored conversation history"
                  >
                    Clear All
                  </button>
                </div>

                <div className="noc-sessions-list">
                  {sessions.map((sess) => {
                    const isActive = sess.id === activeSessionId;
                    const msgCount = sess.messages.length;
                    const lastMsg = sess.messages[sess.messages.length - 1];

                    return (
                      <div
                        key={sess.id}
                        className={`noc-session-card ${isActive ? 'is-active' : ''}`}
                        onClick={() => handleSelectSession(sess.id)}
                      >
                        <div className="noc-session-info">
                          <div className="noc-session-name">
                            {isActive && <span className="noc-active-indicator" />}
                            <span className="noc-session-title-text">{sess.title}</span>
                          </div>
                          <div className="noc-session-meta">
                            <span>{formatDate(sess.updatedAt || sess.createdAt)}</span>
                            <span className="noc-meta-dot">•</span>
                            <span>{msgCount} messages</span>
                          </div>
                          {lastMsg && (
                            <div className="noc-session-preview">
                              {lastMsg.role === 'user' ? 'You: ' : 'NOC: '}
                              {lastMsg.content.slice(0, 90)}
                              {lastMsg.content.length > 90 ? '...' : ''}
                            </div>
                          )}
                        </div>

                        <button
                          type="button"
                          className="noc-session-del-btn"
                          onClick={(e) => handleDeleteSession(e, sess.id)}
                          title="Delete this session"
                          aria-label="Delete session"
                        >
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                          </svg>
                        </button>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : (
              /* Active Chat Conversation View */
              <div className="noc-chat-view">
                {/* Active Chat Meta Bar */}
                <div className="noc-chat-meta-bar">
                  <span className="noc-chat-title-badge" title={activeSession.title}>
                    Current: <strong>{activeSession.title}</strong>
                  </span>
                  <span className="noc-chat-timestamp">
                    Created {formatDate(activeSession.createdAt)}
                  </span>
                </div>

                {/* Active Plugins Strip */}
                {plugins.length > 0 && (
                  <div className="noc-plugins-strip">
                    <span className="noc-plugins-label">Plugins:</span>
                    {plugins.map((plg) => (
                      <span key={plg.name} className="noc-plugin-tag" title={plg.description}>
                        <span className="noc-plugin-dot" />
                        {plg.display_name}
                      </span>
                    ))}
                  </div>
                )}

                {/* Messages List */}
                <div className="noc-messages-scroller">
                  {activeSession.messages.map((msg) => {
                    const isUser = msg.role === 'user';
                    const isErr = msg.isError;

                    return (
                      <div
                        key={msg.id}
                        className={`noc-message-row ${isUser ? 'is-user' : 'is-assistant'} ${isErr ? 'is-error' : ''}`}
                      >
                        <div className="noc-message-avatar">
                          {isUser ? (
                            <span>OP</span>
                          ) : (
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                              <circle cx="12" cy="12" r="3" />
                              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
                            </svg>
                          )}
                        </div>

                        <div className="noc-message-bubble">
                          <div className="noc-bubble-header">
                            <span className="noc-author-name">{isUser ? 'Operator' : 'NOC Agent'}</span>
                            <span className="noc-bubble-time">{formatTime(msg.timestamp)}</span>
                            {!isUser && (
                              <button
                                type="button"
                                className="noc-copy-btn"
                                onClick={() => copyToClipboard(msg.id, msg.content)}
                                title="Copy response to clipboard"
                              >
                                {copiedMsgId === msg.id ? 'Copied' : 'Copy'}
                              </button>
                            )}
                          </div>

                          <div className="noc-bubble-content">
                            {isUser ? (
                              <p className="noc-user-text">{msg.content}</p>
                            ) : (
                              <>
                                <FormattedMessage text={msg.content} />
                                {(msg.traces?.length > 0 || msg.duration_ms > 0) && (
                                  <div className="noc-trace-wrapper">
                                    <button
                                      type="button"
                                      className="noc-trace-toggle"
                                      onClick={() => toggleTrace(msg.id)}
                                      title="Toggle execution telemetry details"
                                    >
                                      <span className="noc-trace-icon">⚡</span>
                                      <span>
                                        {msg.traces?.length > 0
                                          ? `${msg.traces.length} tool${msg.traces.length > 1 ? 's' : ''} called`
                                          : 'Direct response'}
                                      </span>
                                      {msg.duration_ms > 0 && (
                                        <span className="noc-trace-ms">{msg.duration_ms}ms</span>
                                      )}
                                      {msg.compliance_passed && (
                                        <span className="noc-trace-compliance" title="Response verified by NOC Compliance Hook">
                                          ✓ Verified
                                        </span>
                                      )}
                                    </button>

                                    {expandedTraces[msg.id] && msg.traces?.length > 0 && (
                                      <div className="noc-trace-details">
                                        <ul className="noc-trace-list">
                                          {msg.traces.map((tr, tIdx) => (
                                            <li key={tIdx} className="noc-trace-item">
                                              <span className="noc-trace-name">{tr.tool_name || tr.stage}</span>
                                              <div className="noc-trace-meta">
                                                {tr.status === 'success' ? (
                                                  <span style={{ color: '#059669' }}>✓</span>
                                                ) : (
                                                  <span style={{ color: '#dc2626' }}>✗</span>
                                                )}
                                                {tr.duration_ms !== undefined && (
                                                  <span className="noc-trace-ms">{tr.duration_ms}ms</span>
                                                )}
                                              </div>
                                            </li>
                                          ))}
                                        </ul>
                                      </div>
                                    )}
                                  </div>
                                )}
                              </>
                            )}
                          </div>
                        </div>
                      </div>
                    );
                  })}

                  {/* Thinking / Tool Execution Indicator */}
                  {isLoading && (
                    <div className="noc-message-row is-assistant is-thinking">
                      <div className="noc-message-avatar">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <circle cx="12" cy="12" r="3" />
                          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
                        </svg>
                      </div>
                      <div className="noc-message-bubble noc-thinking-bubble">
                        <div className="noc-thinking-dots">
                          <span />
                          <span />
                          <span />
                        </div>
                        <span className="noc-thinking-text">Executing telecom tools & synthesizing diagnosis...</span>
                      </div>
                    </div>
                  )}

                  <div ref={messagesEndRef} />
                </div>

                {/* Quick Prompts Bar */}
                {activeSession.messages.length <= 3 && (
                  <div className="noc-quick-prompts">
                    <span className="noc-quick-label">Operator shortcuts:</span>
                    <div className="noc-quick-chips">
                      {QUICK_PROMPTS.map((qp, idx) => (
                        <button
                          key={idx}
                          type="button"
                          className="noc-chip-btn"
                          onClick={() => handleSendMessage(qp.text)}
                          disabled={isLoading}
                        >
                          {qp.label}
                        </button>
                      ))}
                    </div>
                  </div>
                )}

                {/* Input Area */}
                <div className="noc-input-container">
                  <textarea
                    ref={inputRef}
                    rows={isExpanded ? 3 : 2}
                    value={inputMessage}
                    onChange={(e) => setInputMessage(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ask about grids, rule alerts, ML drops, or citywide metrics... (Enter to send)"
                    disabled={isLoading}
                    className="noc-textarea"
                  />
                  <div className="noc-input-actions">
                    <span className="noc-input-tip">Shift+Enter for newline</span>
                    <button
                      type="button"
                      className="noc-send-btn"
                      onClick={() => handleSendMessage()}
                      disabled={!inputMessage.trim() || isLoading}
                      title="Send message"
                      aria-label="Send"
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <line x1="22" y1="2" x2="11" y2="13" />
                        <polygon points="22 2 15 22 11 13 2 9 22 2" />
                      </svg>
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
