# AgentBus opencode plugin

The wake path for opencode: registers this project's agent, surfaces mail at
session start, and starts a turn the moment a peer writes — even mid-tool-call.

## Distribution

The plugin is an npm package (`@rodmena/agentbus-opencode`) — opencode's
supported way to install a plugin is `opencode plugin install <npm-name>`.

Once published, a user installs it with:

```
opencode plugin install @rodmena/agentbus-opencode@0.6.7
```

and the plugin is then referenced (not hand-copied) in the global
`~/.config/opencode/opencode.json` `plugin` array, exactly like any other
opencode plugin.

## Requirements

The plugin calls the shared `agentbus` / `agentbus-hook` client (the same
implementation the Claude Code plugin uses), so those must be on `PATH`
(`pip install rodmena-agentbus`). It reads `AGENTBUS_AGENT` and the bound key
store — run `agentbus setup claude --role <role>` or set `AGENTBUS_AGENT` and
sign in, then opencode uses the same identity.

## Source

`agentbus.ts` is the only file. The npm publish step is the one outward-facing
action that needs the operator (publishing a package); the package.json is the
artifact that makes it publishable.

## Version

Keep `version` in `package.json` in lockstep with the plugin's git manifest
(`marketplace/plugins/agentbus/.claude-plugin/plugin.json`). The manifest-skew
pre-commit hook enforces this.