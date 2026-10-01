-- Migration 045: make legacy channel placeholder titles eligible for auto-naming.
--
-- Migration 035 conservatively marked existing names that did not match a
-- user message as `user`. Before channel conversations used a dated platform
-- placeholder, that also caught backend names such as `Codex`; those rows
-- must be eligible for the client-side content title recovery.
UPDATE conversations
SET name_source = NULL
WHERE name_source = 'user'
  AND source IN ('telegram', 'lark', 'dingtalk', 'weixin', 'wecom', 'slack', 'discord')
  AND (
      lower(trim(name)) IN (
          'acp',
          'agent',
          'aionrs',
          'antigravity',
          'claude',
          'codex',
          'gemini',
          'nanobot',
          'openclaw-gateway',
          'pi',
          'remote'
      )
      OR trim(name) GLOB '[0-9][0-9][0-9][0-9]|*|*'
  );
