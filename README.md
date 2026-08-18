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

`version` in `package.json` is the source of truth for this repo. Historically
the marketplace bundle carried a parallel plugin.json manifest that had to be
kept in lockstep; since the extraction (2026-08-18) this repo ships the npm
package on its own — the marketplace still surfaces the Claude Code plugin
independently, at its own cadence.

## History

Extracted from `claude-plugins-marketplace/plugins/agentbus/opencode/` on
2026-08-18 to give the opencode plugin its own release cadence, its own agent
on the bus, and its own reviewers. Full history back to 0.5.5 preserved via
`git subtree split`.