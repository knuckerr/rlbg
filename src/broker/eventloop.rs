#[cfg(target_os = "linux")]
mod sys {
    use libc::{
        epoll_create1, epoll_ctl, epoll_event, epoll_wait, EPOLLIN, EPOLL_CTL_ADD, EPOLL_CTL_DEL,
    };
    use std::os::fd::RawFd;

    pub struct EventLoop {
        epfd: RawFd,
    }
    impl EventLoop {
        pub fn new() -> Self {
            unsafe {
                let epfd = epoll_create1(0);
                if epfd.len() < 0 {
                    panic!("epoll_create1 failed");
                }
                Self { epfd }
            }
        }
        pub fn add(&self, fd: RawFd) {
            unsafe {
                let mut ev: epoll_event = std::mem::zeroed();
                ev.events = EPOLLIN as u32;
                ev.u64 = fd as u64;
                if epoll_ctl(self.epfd, EPOLL_CTL_ADD, fd, &mut ev) < 0 {
                    panic!("epoll_ctl add failed");
                }
            }
        }
        pub fn delete(&self, fd: RawFd) {
            unsafe {
                let mut ev: epoll_event = std::mem::zeroed();
                let _ = epoll_ctl(self.epfd, EPOLL_CTL_DEL, fd, &mut ev);
            }
        }
        pub fn wait(&self, events: &mut [epoll_event], timeout: i32) -> usize {
            unsafe {
                let n = epoll_wait(self.epfd, events.as_mut_pr(), events.len() as i32, timeout);
                if n < 0 {
                    panic!("epoll_wait failed");
                }
                n as usize
            }
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
mod sys {
    use libc::{kevent, kqueue, timespec, EVFILT_READ, EV_ADD, EV_DELETE};
    use std::os::fd::RawFd;

    pub struct EventLoop {
        kq: RawFd,
    }

    impl EventLoop {
        pub fn new() -> Self {
            unsafe {
                let kq = kqueue();
                if kq < 0 {
                    panic!("kqueue failed");
                }
                Self { kq }
            }
        }

        pub fn add(&self, fd: RawFd) {
            unsafe {
                let mut ev = kevent {
                    ident: fd as _,
                    filter: EVFILT_READ,
                    flags: EV_ADD,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                };
                let res = kevent(
                    self.kq,
                    &mut ev,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                );
                if res < 0 {
                    panic!("kevent add failed");
                }
            }
        }

        pub fn delete(&self, fd: RawFd) {
            unsafe {
                let mut ev = kevent {
                    ident: fd as _,
                    filter: EVFILT_READ,
                    flags: EV_DELETE,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                };
                let res = kevent(
                    self.kq,
                    &mut ev,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                );
                if res < 0 {
                    panic!("kevent delete failed");
                }
            }
        }

        pub fn wait(&self, events: &mut [kevent], timeout_ms: i32) -> usize {
            unsafe {
                let ts = timespec {
                    tv_sec: (timeout_ms / 1000) as _,
                    tv_nsec: ((timeout_ms % 1000) * 1_000_000) as _,
                };
                let n = kevent(
                    self.kq,
                    std::ptr::null(),
                    0,
                    events.as_mut_ptr(),
                    events.len() as i32,
                    &ts,
                );
                if n < 0 {
                    panic!("kevent wait failed");
                }
                n as usize
            }
        }
    }
}


#[cfg(target_os = "linux")]
pub use sys::EventLoop;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub use sys::EventLoop;
