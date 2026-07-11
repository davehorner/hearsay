use enum2contract::EnumContract;
use serde::{Deserialize, Serialize};

#[derive(Debug, EnumContract, Serialize, Deserialize)]
pub enum BrokerContract {
    #[topic("hearsay/peers/connected")]
    PeerConnected { id: String },

    #[topic("hearsay/peers/request")]
    RequestPeers,

    #[topic("hearsay/peers/report")]
    ReportPeers { peers: Vec<String> },

    #[topic("hearsay/subscriptions/request")]
    RequestSubscriptions,

    #[topic("hearsay/subscriptions/report")]
    ReportSubscriptions {
        subscriptions: Vec<(String, Vec<String>)>,
    },

    #[topic("hearsay/bridges/created")]
    BridgeCreated {
        id: String,
        source_address: String,
        target_address: String,
    },

    #[topic("hearsay/bridges/request")]
    RequestBridges,

    #[topic("hearsay/bridges/report")]
    ReportBridges { bridges: Vec<String> },
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Route {
    #[default]
    Global,

    Local,
}
