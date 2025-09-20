use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug)]
pub enum Level {
    Info,
    Warn,
    Error,
    Debug,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Level::Info => write!(f, "INFO"),
            Level::Warn => write!(f, "Warn"),
            Level::Error => write!(f, "Error"),
            Level::Debug => write!(f, "Debug"),
        }
    }
}


#[derive(Debug)]
pub struct Logger {
    sender: Arc<(Mutex<VecDeque<String>>, Condvar)>,
    _handle: JoinHandle<()>,
}

impl Logger {
    pub fn new() -> Self {
        let queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let queue_clone = queue.clone();

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*queue_clone;
            loop {
                let mut q = lock.lock().unwrap();
                while q.is_empty() {
                    q = cvar.wait(q).unwrap();
                }
                while let Some(msg) = q.pop_front() {
                    let _ = writeln!(io::stdout(), "{}", msg);
                }
            }
        });
        Self {
            sender: queue,
            _handle: handle,
        }
    }

    pub fn log_fmt(&self, level: Level, args: fmt::Arguments) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let formatted = format!("[{}][{}] {}", now, level, args);

        let (lock, cvar) = &*self.sender;
        let mut q = lock.lock().unwrap();
        q.push_back(formatted);
        cvar.notify_one();
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::new()
    }
}

static LOGGER: OnceLock<Logger> = OnceLock::new();

pub fn init_logger() {
    LOGGER.set(Logger::new()).unwrap();
}

pub fn global_loger() -> &'static Logger {
    LOGGER.get().expect("Logger not initialized")
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($arg:tt)*) => {
        $logger.log_fmt($crate::logger::Level::Info, format_args!($($arg)*));
    };
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($arg:tt)*) => {
        $logger.log_fmt($crate::logger::Level::Warn, format_args!($($arg)*));
    };
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($arg:tt)*) => {
        $logger.log_fmt($crate::logger::Level::Error, format_args!($($arg)*));
    };
}

#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($arg:tt)*) => {
        $logger.log_fmt($crate::logger::Level::Debug, format_args!($($arg)*));
    };
}
