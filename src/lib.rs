//! Crate `rustin_redis` provides a Redis data store for the Rustin chat bot.

#![deny(missing_docs)]
#![feature(futures_api)]

use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;

use futures::prelude::*;
use futures::compat::Future01CompatExt;
use redis::{
    r#async::Connection,
    Client,
    RedisError,
};
use rustin::store::{ScopedStore, Store};

/// A trait used by callbacks to require an accessible Redis database.
pub trait StoreExt {
    /// Returns a connection to Redis.
    fn redis(&self) -> Box<dyn Future<Output = Result<Connection, RedisError>>>;
}

/// A Redis database as a Rustin `Store`.
#[derive(Clone, Debug)]
pub struct Redis(pub Client);

impl StoreExt for Redis {
    fn redis(&self) -> Box<dyn Future<Output = Result<Connection, RedisError>>> {
        Box::new(self.0.get_async_connection().compat())
    }
}

impl Store for Redis {
    type Error = RedisError;

    /// Gets the value of the given key, if any.
    fn get<K>(&self, key: K) -> Pin<Box<dyn Future<Output = Result<Option<String>, Self::Error>>>>
    where
        K: AsRef<str> + Display
    {
        let connection = self.0.get_async_connection();
        let key = key.as_ref().to_owned();

        Box::pin(connection.compat().and_then(move |connection| {
            redis::cmd("GET")
                .arg(key)
                .query_async(connection)
                .compat()
                .map_ok(|response| response.1)
        }))
    }
    /// Sets the given key to the given value.
    fn set<K, V>(&self, key: K, value: V) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>>>>
    where
        K: Display + Into<String>,
        V: Into<String>
    {
        let connection = self.0.get_async_connection();
        let key = key.into();
        let value = value.into();

        Box::pin(connection.compat().and_then(move |connection| {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .query_async(connection)
                .compat()
                .map_ok(|response| response.1)
        }))
    }

    /// Creates a new `Store` that prepends the given prefix to all key names.
    fn scoped<P>(&self, prefix: P) -> ScopedStore<Self>
    where
        P: Into<String>,
        Self: Sized
    {
        ScopedStore::new(self.clone(), prefix)
    }
}
