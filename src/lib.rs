pub mod airlock;
pub mod twine_plugin;
pub mod worker_pool;

pub use twine_plugin::TwineGeyserPlugin;

// Main entry point for the plugin
#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin(
) -> *mut dyn agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin {
    Box::into_raw(Box::new(TwineGeyserPlugin::default()))
}
