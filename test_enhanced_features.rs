// Test to verify enhanced notification methods are available
use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

fn main() {
    println!("Testing enhanced notification methods availability...");
    
    // This will fail to compile if the methods are not available
    let plugin = twine_solana_geyser::TwineGeyserPlugin::default();
    
    // Check if the methods exist
    let _ = plugin.account_change_notifications_enabled();
    let _ = plugin.slot_lthash_notifications_enabled();
    let _ = plugin.bank_hash_components_notifications_enabled();
    
    println!("All enhanced notification methods are available!");
}