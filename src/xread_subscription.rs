use std::{
    collections::HashSet,
    sync::{mpsc::Sender, Arc},
};

use crate::Server;

pub struct XreadSubscription {
    wakeup_keys: HashSet<String>,
    wakeup_sender: Sender<()>,
}

impl XreadSubscription {
    pub fn new(wakeup_keys: HashSet<String>, wakeup_sender: Sender<()>) -> XreadSubscription {
        return XreadSubscription {
            wakeup_keys,
            wakeup_sender,
        };
    }

    pub fn attempt_wakeup(&self, key: &String) -> bool {
        if self.wakeup_keys.contains(key) {
            self.wakeup_sender.send(()).unwrap();
            return true;
        }
        return false;
    }
}

pub fn wakeup_subscribers(server_info: &Arc<Server>, key: &String) {
    let mut subscribers = server_info.xread_subscriptions.write().unwrap();
    subscribers.retain(|subscriber| !subscriber.attempt_wakeup(key));
}
