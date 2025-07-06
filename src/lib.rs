pub mod airlock;
pub mod api_server;
pub mod chain_monitor;
pub mod logging;
pub mod metrics_server;
pub mod twine_plugin;
pub mod worker_pool;

pub use metrics_server::MetricsServer;
pub use twine_plugin::TwineGeyserPlugin;

// Main entry point for the plugin
#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin(
) -> *mut dyn agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin {
    // Initialize basic logging to file for plugin creation
    // Full logging will be configured in on_load when we have the config
    logging::init_basic_file_logging("twine-geyser-plugin.log");

    log::info!("Creating Twine Geyser Plugin instance");

    let plugin = TwineGeyserPlugin::default();
    let plugin: Box<dyn agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin> =
        Box::new(plugin);

    let raw_ptr = Box::into_raw(plugin);
    log::info!("Twine Geyser Plugin instance created at {:p}", raw_ptr);

    raw_ptr
}
