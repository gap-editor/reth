use std::future::Future;

use alloy_primitives::B256;
use auto_impl::auto_impl;
use reth_eth_wire_types::BlockAccessLists;

use crate::{download::DownloadClient, error::PeerRequestResult, priority::Priority};

/// A client for fetching block access lists from peers over the eth/71 protocol.
///
/// This is analogous to [`BodiesClient`](crate::bodies::client::BodiesClient) but for
/// [`GetBlockAccessLists`](reth_eth_wire_types::GetBlockAccessLists) /
/// [`BlockAccessLists`](reth_eth_wire_types::BlockAccessLists) messages.
///
/// # eth/71 only
///
/// Block access lists are only available via the eth/71 protocol. Implementations must ensure that
/// requests are only sent to peers that have negotiated eth/71 capability.
#[auto_impl(&, Arc, Box)]
pub trait BlockAccessListsClient: DownloadClient {
    /// The output type for block access lists requests.
    type Output: Future<Output = PeerRequestResult<BlockAccessLists>> + Send + Sync + Unpin;

    /// Fetches block access lists for the given block hashes from a suitable peer.
    ///
    /// The returned [`BlockAccessLists`] will contain the access lists for the requested blocks,
    /// in the same order as the input hashes.
    fn get_block_access_lists(&self, hashes: Vec<B256>) -> Self::Output {
        self.get_block_access_lists_with_priority(hashes, Priority::Normal)
    }

    /// Fetches block access lists for the given block hashes with a specific priority.
    ///
    /// Higher priority requests will be dispatched before lower priority ones.
    fn get_block_access_lists_with_priority(
        &self,
        hashes: Vec<B256>,
        priority: Priority,
    ) -> Self::Output;
}
