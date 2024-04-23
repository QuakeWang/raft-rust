use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};

// Timer internal thread checking interval
const THREAD_CHECK_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub struct Timer {
    // The name of timer
    name: String,
    // To control timer running
    alive: Arc<AtomicBool>,
    // The interval of timer to trigger
    trigger_interval: Arc<Mutex<Duration>>,
    // The next trigger time
    next_trigger: Arc<Mutex<Instant>>,
    // The last reset time
    pub last_reset_at: Option<Instant>,
    // The thread handle in timer
    handle: Option<JoinHandle<()>>,
}

impl Timer {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            alive: Arc::new(AtomicBool::new(false)),
            trigger_interval: Arc::new(Mutex::new(Duration::from_secs(u64::MAX))),
            next_trigger: Arc::new(Mutex::new(Instant::now())),
            last_reset_at: None,
            handle: None,
        }
    }

    // Start the timer schedule
    pub fn schedule<F>(&mut self, trigger_interval: Duration, callback: F)
    where
        F: 'static + Send + Clone + FnMut() -> (),
    {
        info!(
            "{} start schedule with trigger interval: {}ms",
            self.name,
            trigger_interval.as_millis()
        );

        (*self.trigger_interval.lock().unwrap()) = trigger_interval;
        (*self.next_trigger.lock().unwrap()) = Instant::now() + trigger_interval;
        self.alive.store(true, Ordering::SeqCst);

        let trigger_interval = self.trigger_interval.clone();
        let next_trigger = self.next_trigger.clone();
        let alive = self.alive.clone();

        self.handle = Some(std::thread::spawn(move || {
            loop {
                std::thread::sleep(THREAD_CHECK_INTERVAL);

                if !alive.load(Ordering::SeqCst) {
                    break;
                }

                if (*next_trigger.lock().unwrap()) <= Instant::now() {
                    // async callback function will not block the thread
                    let mut callback = callback.clone();
                    std::thread::spawn(move || {
                        callback();
                    });

                    // calculate next trigger time
                    (*next_trigger.lock().unwrap()) =
                        Instant::now() + (*trigger_interval.lock().unwrap());
                }
            }
        }));
    }

    /// Stop the timer
    pub fn stop(&mut self) {
        info!("{} stopping", self.name);
        self.alive.store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }

    // Reset the timer interval
    pub fn reset(&mut self, trigger_interval: Duration) {
        info!(
            "{} reset with trigger interval: {}ms",
            self.name,
            trigger_interval.as_millis()
        );
        self.last_reset_at = Some(Instant::now());
        *self.trigger_interval.lock().unwrap() = trigger_interval;
        *self.next_trigger.lock().unwrap() = Instant::now() + trigger_interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer() {
        let mut timer = Timer::new("test");
        timer.schedule(Duration::from_millis(100), || {
            println!("timer triggered");
        });

        sleep(Duration::from_millis(200));
        timer.stop();
    }

    #[test]
    fn test_timer_reset() {
        let mut timer = Timer::new("test");
        timer.schedule(Duration::from_millis(100), || {
            println!("timer triggered");
        });

        sleep(Duration::from_millis(200));
        timer.reset(Duration::from_millis(200));

        sleep(Duration::from_millis(300));
        timer.stop();
    }
}
