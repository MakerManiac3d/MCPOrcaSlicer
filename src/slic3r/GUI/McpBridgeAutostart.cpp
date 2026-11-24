#include "McpBridgeAutostart.hpp"

#include "GUI_App.hpp"
#include "libslic3r/AppConfig.hpp"
#include "libslic3r/Semver.hpp"
#include "libslic3r/Utils.hpp"
#include "slic3r/plugin/PluginDescriptor.hpp"
#include "slic3r/plugin/PluginManager.hpp"

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/log/trivial.hpp>
#include <boost/nowide/fstream.hpp>
#include <boost/system/error_code.hpp>
#include <wx/app.h>

#include <regex>
#include <string>
#include <vector>

namespace Slic3r::GUI {

namespace {

const char* const BRIDGE_ENTRY_FILE  = "mcp_bridge.py";
const char* const BRIDGE_SCRIPT_NAME = "MCP Bridge: servidor";

boost::filesystem::path bundled_bridge_path()
{
    return boost::filesystem::path(resources_dir()) / "mcp" / BRIDGE_ENTRY_FILE;
}

// The `version = "x.y.z"` line of the plugin's PEP 723 header.
std::string read_header_version(const boost::filesystem::path& file)
{
    boost::nowide::ifstream in(file.string());
    static const std::regex version_re(R"(^#\s*version\s*=\s*"([^"]+)\")");
    std::string             line;
    std::smatch             match;
    while (std::getline(in, line)) {
        if (line.rfind("# ///", 0) == 0 && line.size() > 5 && line.find("script") == std::string::npos)
            break; // end of the header block
        if (std::regex_search(line, match, version_re))
            return match[1].str();
    }
    return {};
}

// The installed local copy of the bridge, matched by its entry file name.
bool find_installed_bridge(PluginDescriptor& out)
{
    for (PluginDescriptor& descriptor : PluginManager::instance().get_plugin_descriptors(true))
        if (!descriptor.is_cloud_plugin() && boost::filesystem::path(descriptor.entry_path).filename() == BRIDGE_ENTRY_FILE) {
            out = std::move(descriptor);
            return true;
        }
    return false;
}

bool is_newer(const std::string& bundled, const std::string& installed)
{
    const auto b = Semver::parse(bundled);
    const auto i = Semver::parse(installed);
    if (b && i)
        return *b > *i;
    return bundled != installed;
}

} // namespace

void mcp_install_bundled_bridge()
{
    const boost::filesystem::path bundled = bundled_bridge_path();
    boost::system::error_code     ec;
    if (!boost::filesystem::is_regular_file(bundled, ec))
        return;

    const std::string bundled_version = read_header_version(bundled);
    PluginDescriptor  installed;
    if (find_installed_bridge(installed)) {
        const std::string& installed_version = installed.installed_version.empty() ? installed.version : installed.installed_version;
        if (!is_newer(bundled_version, installed_version))
            return;
        BOOST_LOG_TRIVIAL(info) << "MCP Bridge: updating plugin " << installed_version << " -> " << bundled_version;
    } else {
        BOOST_LOG_TRIVIAL(info) << "MCP Bridge: installing bundled plugin " << bundled_version;
    }

    PluginManager& plugin_mgr = PluginManager::instance();
    std::string    error;
    if (!plugin_mgr.install_plugin(bundled, error)) {
        BOOST_LOG_TRIVIAL(error) << "MCP Bridge: bundled plugin install failed: " << error;
        return;
    }
    plugin_mgr.rescan_plugins();
}

void mcp_wire_bridge_autostart()
{
    PluginManager::instance().subscribe_on_load_callback([](const std::string& plugin_key) {
        if (wxTheApp == nullptr || wxGetApp().is_closing() || wxGetApp().app_config == nullptr)
            return;
        if (wxGetApp().app_config->get("mcp_bridge_autostart") == "0")
            return;
        PluginDescriptor descriptor;
        if (!PluginManager::instance().try_get_plugin_descriptor(plugin_key, descriptor) || descriptor.is_cloud_plugin() ||
            boost::filesystem::path(descriptor.entry_path).filename() != BRIDGE_ENTRY_FILE)
            return;
        // Start it once the load (and its other on-load callbacks) has finished.
        wxGetApp().CallAfter([plugin_key] {
            if (wxGetApp().is_closing())
                return;
            const auto launch = PluginManager::instance().launch_script_capability(plugin_key, BRIDGE_SCRIPT_NAME);
            if (!launch.launched_async)
                BOOST_LOG_TRIVIAL(error) << "MCP Bridge: autostart failed: " << launch.outcome.error;
            else
                BOOST_LOG_TRIVIAL(info) << "MCP Bridge: server started";
        });
    });
}

} // namespace Slic3r::GUI
