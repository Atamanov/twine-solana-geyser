pub mod aggregator;
pub mod airlock;
pub mod api;
pub mod db_writer;
pub mod metrics;
pub mod plugin;
pub mod rpc_poller;

#[cfg(feature = "tests-only")]
mod tests;

#[cfg(feature = "tests-only")]
mod test_utils;

#[cfg(feature = "tests-only")]
mod test_harness;

pub use plugin::TwineGeyserPlugin;

/// Export the plugin entry point
#[allow(improper_ctypes_definitions)]
#[no_mangle]

pub extern "C" fn _create_plugin(
) -> *mut dyn agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin {
    let plugin = TwineGeyserPlugin::new();
    let plugin: Box<dyn agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin> =
        Box::new(plugin);
    Box::into_raw(plugin)
}
