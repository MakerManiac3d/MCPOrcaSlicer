#pragma once

namespace Slic3r::GUI {

// MCPOrcaSlicer ships the "MCP Bridge" plugin with the application (resources/mcp/mcp_bridge.py) so AI agents can
// drive it over MCP without a manual plugin install.

// Installs the bundled plugin when it is missing or older than the bundled copy. Call after plugin discovery and
// before the enabled plugins are loaded.
void mcp_install_bundled_bridge();

// Starts the bridge server script each time the bundled plugin loads, unless the user turned off the
// "mcp_bridge_autostart" app setting.
void mcp_wire_bridge_autostart();

} // namespace Slic3r::GUI
