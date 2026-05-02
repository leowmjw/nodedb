// SPDX-License-Identifier: Apache-2.0

//! Bounded connection pool for native protocol connections.
//!
//! Uses a semaphore for max-size enforcement and a std::sync::Mutex
//! idle queue (so Drop can return connections synchronously).
//! Health checks (ping) are performed on idle connections before
//! handing them out.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{AuthMethod, Limits};
use tokio::sync::{Semaphore, SemaphorePermit};

use super::connection::NativeConnection;

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Server address (host:port).
    pub addr: String,
    /// Maximum number of connections.
    pub max_size: usize,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Idle connection timeout (connections idle longer than this are dropped).
    pub idle_timeout: Duration,
    /// Authentication method.
    pub auth: AuthMethod,
    /// Target database name sent in the auth handshake frame.
    ///
    /// `None` means the server default (`"default"` / `DatabaseId::DEFAULT`).
    pub database: Option<String>,
    /// TLS configuration. Default: disabled.
    pub tls: super::connection::TlsConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:6433".into(),
            max_size: 10,
            connect_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            auth: AuthMethod::Trust {
                username: "admin".into(),
            },
            database: None,
            tls: Default::default(),
        }
    }
}

/// Negotiated connection metadata from the first handshake performed by this pool.
#[derive(Debug, Clone, Default)]
pub struct NegotiatedMeta {
    pub proto_version: u16,
    pub capabilities: u64,
    pub server_version: String,
    pub limits: Limits,
}

/// Shared state for idle connection return.
///
/// Uses `std::sync::Mutex` (not tokio) so the `Drop` impl can
/// return connections synchronously without spawning async tasks.
struct PoolInner {
    idle: Mutex<VecDeque<NativeConnection>>,
    /// Negotiated metadata from the first handshake.
    meta: Mutex<Option<NegotiatedMeta>>,
    max_size: usize,
}

/// A bounded pool of `NativeConnection` instances.
pub struct Pool {
    config: PoolConfig,
    inner: Arc<PoolInner>,
    semaphore: Semaphore,
}

impl Pool {
    /// Create a new pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        let max_size = config.max_size;
        let semaphore = Semaphore::new(max_size);
        Self {
            config,
            inner: Arc::new(PoolInner {
                idle: Mutex::new(VecDeque::new()),
                meta: Mutex::new(None),
                max_size,
            }),
            semaphore,
        }
    }

    /// Returns the negotiated connection metadata from the first handshake, or
    /// `None` if no connection has been established yet.
    pub fn negotiated_meta(&self) -> Option<NegotiatedMeta> {
        self.inner
            .meta
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Acquire a connection from the pool.
    ///
    /// Returns an idle connection if available, otherwise creates a new one.
    /// Blocks if `max_size` connections are already in use.
    pub async fn acquire(&self) -> NodeDbResult<PooledConnection<'_>> {
        let permit = tokio::time::timeout(self.config.connect_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| NodeDbError::sync_connection_failed("pool acquire timeout"))?
            .map_err(|_| NodeDbError::sync_connection_failed("pool closed"))?;

        // std::sync::Mutex is intentional here (not tokio::sync::Mutex):
        // 1. Critical section is trivial (pop_front / push_back only)
        // 2. No async operations while holding the lock
        // 3. Enables synchronous return in Drop (no spawned tasks)
        // 4. Poison is handled gracefully via unwrap_or_else
        let idle_conn = {
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            idle.pop_front()
        };

        if let Some(mut conn) = idle_conn {
            // Health check: ping to verify the connection is still alive.
            if conn.ping().await.is_ok() {
                return Ok(PooledConnection {
                    conn: Some(conn),
                    inner: Arc::clone(&self.inner),
                    _permit: permit,
                });
            }
            // Connection dead — create a new one below.
        }

        // Create a new connection (plain TCP or TLS).
        let addr = self.config.addr.clone();
        let tls_cfg = self.config.tls.clone();
        let timeout = self.config.connect_timeout;
        let mut conn = tokio::time::timeout(timeout, async move {
            if tls_cfg.enabled {
                NativeConnection::connect_tls(&addr, &tls_cfg).await
            } else {
                NativeConnection::connect(&addr).await
            }
        })
        .await
        .map_err(|_| NodeDbError::sync_connection_failed("connect timeout"))??;

        // `NativeConnection::connect` already performed the handshake.
        // Calling it a second time here would write a stale HelloFrame
        // into the post-handshake stream and confuse the framed read
        // path (the bytes get mis-parsed as a regular response frame).

        // Authenticate — pass the optional database name for handshake binding.
        conn.authenticate(self.config.auth.clone(), self.config.database.as_deref())
            .await?;

        // Capture negotiated metadata from the first handshake.
        {
            let mut meta = self.inner.meta.lock().unwrap_or_else(|e| e.into_inner());
            if meta.is_none() {
                *meta = Some(NegotiatedMeta {
                    proto_version: conn.proto_version,
                    capabilities: conn.capabilities,
                    server_version: conn.server_version.clone(),
                    limits: conn.limits.clone(),
                });
            }
        }

        Ok(PooledConnection {
            conn: Some(conn),
            inner: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

/// A connection borrowed from the pool.
///
/// Returns to the idle queue synchronously on drop (no spawned tasks).
pub struct PooledConnection<'a> {
    conn: Option<NativeConnection>,
    inner: Arc<PoolInner>,
    _permit: SemaphorePermit<'a>,
}

impl std::ops::Deref for PooledConnection<'_> {
    type Target = NativeConnection;
    fn deref(&self) -> &NativeConnection {
        self.conn.as_ref().expect("connection taken")
    }
}

impl std::ops::DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut NativeConnection {
        self.conn.as_mut().expect("connection taken")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Synchronous return to idle queue — no async, no spawned tasks.
            let mut idle = self.inner.idle.lock().unwrap_or_else(|e| e.into_inner());
            if idle.len() < self.inner.max_size {
                idle.push_back(conn);
            }
            // else: too many idle connections — drop this one.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_config_defaults() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.addr, "127.0.0.1:6433");
        assert_eq!(cfg.max_size, 10);
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    }

    #[test]
    fn pool_creates_semaphore() {
        let pool = Pool::new(PoolConfig {
            max_size: 5,
            ..Default::default()
        });
        assert_eq!(pool.semaphore.available_permits(), 5);
    }

    /// Regression test: pool must not call `perform_client_handshake()` twice.
    ///
    /// `NativeConnection::connect()` already performs the handshake internally.
    /// A previous bug in `Pool::acquire()` called it a second time immediately
    /// after `connect()` returned, causing the server — now in frame-read mode —
    /// to see `NDBH` (0x4E44_4248 = 1313096264) as a frame-length prefix and
    /// reject the connection with "frame size 1313096264 exceeds maximum 16777216".
    ///
    /// This test catches that by asserting the bytes immediately following the
    /// HelloAck are a regular frame-length prefix, not a second HelloFrame magic.
    #[tokio::test]
    async fn pool_does_not_send_double_handshake() {
        use nodedb_types::protocol::{HelloAckFrame, HELLO_MAGIC, NativeResponse};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            // Read the one and only HelloFrame (16 raw bytes, no length prefix).
            let mut hello_buf = [0u8; 16];
            stream.read_exact(&mut hello_buf).await.unwrap();
            let magic = u32::from_be_bytes([
                hello_buf[0], hello_buf[1], hello_buf[2], hello_buf[3],
            ]);
            assert_eq!(magic, HELLO_MAGIC, "first message should be a HelloFrame");

            // Reply with a minimal HelloAckFrame.
            let ack = HelloAckFrame {
                proto_version: 1,
                capabilities: 0,
                server_version: "NodeDB/test".into(),
                limits: Limits::default(),
            }
            .encode();
            stream.write_all(&ack).await.unwrap();
            stream.flush().await.unwrap();

            // Read the next 4 bytes. These must be the frame-length prefix of
            // the Auth request — NOT another NDBH magic (the double-handshake bug).
            let mut next4 = [0u8; 4];
            stream.read_exact(&mut next4).await.unwrap();
            let next_u32 = u32::from_be_bytes(next4);
            assert_ne!(
                next_u32, HELLO_MAGIC,
                "pool sent a second HelloFrame (double-handshake regression): \
                 server saw 0x{next_u32:08X} = {next_u32} where a frame-length was expected"
            );

            // Drain the Auth payload and reply with auth-ok so the pool succeeds.
            let mut payload = vec![0u8; next_u32 as usize];
            stream.read_exact(&mut payload).await.unwrap();

            let resp = NativeResponse::auth_ok(1, "test".into(), 0);
            let encoded = zerompk::to_msgpack_vec(&resp).unwrap();
            let len = (encoded.len() as u32).to_be_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&encoded).await.unwrap();
            stream.flush().await.unwrap();
        });

        let pool = Pool::new(PoolConfig {
            addr: addr.to_string(),
            max_size: 1,
            auth: AuthMethod::Trust {
                username: "test".into(),
            },
            ..Default::default()
        });

        // acquire() runs connect() (which handshakes) then authenticate().
        // This is the exact sequence that triggered the double-handshake bug.
        let _conn = pool.acquire().await.expect("pool acquire should succeed");

        // Server task will panic if it saw a second HelloFrame.
        server.await.unwrap();
    }
}
