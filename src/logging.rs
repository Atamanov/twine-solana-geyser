use env_logger::Builder;
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;

static LOGGER_INITIALIZED: Mutex<bool> = Mutex::new(false);

/// Initialize basic file logging with INFO level
pub fn init_basic_file_logging(log_file: &str) {
    let mut initialized = LOGGER_INITIALIZED.lock().unwrap();
    if *initialized {
        return;
    }

    // Try to open/create the log file
    if let Ok(log_file) = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(log_file)
    {
        // Initialize logger with file output
        let _ = Builder::new()
            .target(env_logger::Target::Pipe(Box::new(log_file)))
            .filter_level(LevelFilter::Info)
            .format(|buf, record| {
                writeln!(
                    buf,
                    "[{} {} {}:{}] {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                    record.level(),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .try_init();
    } else {
        // Fallback to stderr if file can't be opened
        let _ = env_logger::try_init();
    }

    *initialized = true;
}

/// Initialize logging with custom configuration
pub fn init_configured_logging(log_file: Option<&str>, log_level: &str) {
    let mut initialized = LOGGER_INITIALIZED.lock().unwrap();
    if *initialized {
        // Logger already initialized, just log that we're keeping the existing setup
        log::info!("Logger already initialized, keeping existing configuration");
        return;
    }

    let level_filter = match log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    let mut builder = Builder::new();
    builder.filter_level(level_filter);

    // Configure output target
    if let Some(file_path) = log_file {
        if let Ok(log_file) = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_path)
        {
            builder.target(env_logger::Target::Pipe(Box::new(log_file)));
            builder.format(|buf, record| {
                writeln!(
                    buf,
                    "[{} {} {}:{}] {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                    record.level(),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            });
        }
    } else {
        // Log to stderr if no file specified
        builder.format(|buf, record| {
            writeln!(
                buf,
                "[{} {} {}:{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.level(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            )
        });
    }

    let _ = builder.try_init();
    *initialized = true;
}
