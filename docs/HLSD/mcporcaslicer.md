# MCPOrcaSlicer

MCPOrcaSlicer is a fork of OrcaSlicer that AI agents can drive over MCP (Model Context Protocol) without
taking over the screen. It runs beside a regular OrcaSlicer install and looks different enough to tell
the two apart.

## Identity

- `SLIC3R_APP_FULL_NAME` ("MCPOrcaSlicer") is the name users see: window title, About dialog and message
  boxes. `MCP_ORCA_VERSION` (`<SoftFever_VERSION>-<MCP_ORCA_VERSION_SUFFIX>`, set in `version.inc`) is the
  version shown on the splash screen and in About.
- `SLIC3R_APP_NAME` and `SLIC3R_APP_KEY` are unchanged: .3mf files record the former and the translation
  catalogs are named after the latter, so changing them would break project and translation compatibility.
- The palette is red (#C62828) instead of teal. `scripts/mcp_rebrand.py` rewrites the colours in GUI
  sources, SVG/web resources and the logo bitmaps; its output is committed. Run it again after merging
  upstream changes (`--check` reports what is left).

## User data

The data folder is `MCP_APP_DATA_KEY` ("MCPOrcaSlicer") instead of "OrcaSlicer", so presets, plugins and
settings are not shared with OrcaSlicer. On the first start, `seed_data_dir_from_orcaslicer()` (GUI_App.cpp)
copies the OrcaSlicer folder next to it, skipping logs, caches, OTA data, backups, lock files and the live
bridge state. A failed copy is logged and start-up continues with an empty folder. A portable `data_dir`
beside the executable still takes precedence.

## Bundled MCP bridge

`resources/mcp/mcp_bridge.py` is the "MCP Bridge" plugin. At start-up `mcp_install_bundled_bridge()`
installs it through `PluginManager::install_plugin()` when it is missing or older than the bundled copy
(compared by the `version` of its PEP 723 header), and `mcp_wire_bridge_autostart()` starts its
"MCP Bridge: servidor" script whenever the plugin loads. The `mcp_bridge_autostart` app setting set to `0`
turns the autostart off.

The plugin serves a local HTTP endpoint (127.0.0.1 + token) that the external MCP server calls. It writes
its address to `orca_mcp_bridge.json` in its plugin data folder; the MCP server looks for that file in the
MCPOrcaSlicer folder and then in the OrcaSlicer one.

## Live edit API (`orca.host.edit`)

`src/slic3r/plugin/host/PluginHostEdit.cpp` gives plugins write access to the open project: transforms,
per-object and per-part settings, parts, painting, cut, layer ranges, per-layer G-code, brim ears, plate
settings, project information and project settings. Each function converts its arguments first and then
runs on the UI thread through `PluginHostUi::run_on_ui_thread`, and refreshes the GUI through the same
Plater/ObjectList paths the user's own actions use.

Each call is one undo step. `begin_undo_group()`/`end_undo_group()` merge the calls in between into a
single step, the way a gizmo session does: one snapshot at the start, snapshots suppressed until the
outermost end. `call_on_ui(fn)` runs a Python callable on the UI thread, so a plugin can read the model
from its script thread without a UI page pumping calls.
