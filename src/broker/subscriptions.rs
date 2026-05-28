use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::protocol::{Packet, QoS};

#[derive(Debug, Clone)]
pub(crate) struct Subscriber {
    pub client_id: String,
    pub sender: mpsc::UnboundedSender<Packet>,
    pub qos: QoS,
}

#[derive(Debug, Default)]
struct TopicNode {
    children: HashMap<String, TopicNode>,
    single_wildcard: Option<Box<TopicNode>>,
    multi_wildcard_subscribers: Vec<Subscriber>,
    subscribers: Vec<Subscriber>,
}

#[derive(Debug, Default)]
pub(crate) struct TopicTree {
    root: TopicNode,
}

impl TopicTree {
    pub fn add(&mut self, filter: &str, subscriber: Subscriber) {
        let parts: Vec<&str> = filter.split('/').collect();
        Self::add_recursive(&mut self.root, &parts, subscriber);
    }

    fn add_recursive(node: &mut TopicNode, parts: &[&str], subscriber: Subscriber) {
        if parts.is_empty() {
            node.subscribers.push(subscriber);
            return;
        }

        let part = parts[0];
        let remaining = &parts[1..];

        if part == "#" {
            node.multi_wildcard_subscribers.push(subscriber);
        } else if part == "+" {
            let child = node
                .single_wildcard
                .get_or_insert_with(|| Box::new(TopicNode::default()));
            Self::add_recursive(child, remaining, subscriber);
        } else {
            let child = node
                .children
                .entry(part.to_string())
                .or_default();
            Self::add_recursive(child, remaining, subscriber);
        }
    }

    pub fn remove(&mut self, filter: &str, client_id: &str) {
        let parts: Vec<&str> = filter.split('/').collect();
        Self::remove_recursive(&mut self.root, &parts, client_id);
    }

    fn remove_recursive(
        node: &mut TopicNode,
        parts: &[&str],
        client_id: &str,
    ) -> bool {
        if parts.is_empty() {
            node.subscribers.retain(|s| s.client_id != client_id);
            return true;
        }

        let part = parts[0];
        let remaining = &parts[1..];

        if part == "#" {
            node.multi_wildcard_subscribers
                .retain(|s| s.client_id != client_id);
        } else if part == "+" {
            if let Some(ref mut child) = node.single_wildcard {
                Self::remove_recursive(child, remaining, client_id);
            }
        } else if let Some(child) = node.children.get_mut(part) {
            Self::remove_recursive(child, remaining, client_id);
        }

        false
    }

    pub fn remove_all_for_client(&mut self, client_id: &str) {
        Self::remove_all_recursive(&mut self.root, client_id);
    }

    fn remove_all_recursive(node: &mut TopicNode, client_id: &str) {
        node.subscribers.retain(|s| s.client_id != client_id);
        node.multi_wildcard_subscribers
            .retain(|s| s.client_id != client_id);

        if let Some(ref mut child) = node.single_wildcard {
            Self::remove_all_recursive(child, client_id);
        }

        for child in node.children.values_mut() {
            Self::remove_all_recursive(child, client_id);
        }
    }

    pub fn matches(&self, topic: &str) -> Vec<Subscriber> {
        let parts: Vec<&str> = topic.split('/').collect();
        let mut result = Vec::new();
        self.matches_recursive(&self.root, &parts, 0, &mut result);
        result
    }

    fn matches_recursive(
        &self,
        node: &TopicNode,
        parts: &[&str],
        depth: usize,
        result: &mut Vec<Subscriber>,
    ) {
        result.extend(node.subscribers.clone());

        result.extend(node.multi_wildcard_subscribers.clone());

        if depth >= parts.len() {
            return;
        }

        let current = parts[depth];

        if let Some(ref child) = node.single_wildcard {
            self.matches_recursive(child, parts, depth + 1, result);
        }

        if let Some(child) = node.children.get(current) {
            self.matches_recursive(child, parts, depth + 1, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_subscriber(client_id: &str) -> Subscriber {
        let (tx, _rx) = mpsc::unbounded_channel();
        Subscriber {
            client_id: client_id.to_string(),
            sender: tx,
            qos: QoS::AtMostOnce,
        }
    }

    #[test]
    fn exact_match() {
        let mut tree = TopicTree::default();
        tree.add("sensor/temp", make_subscriber("client1"));
        let matches = tree.matches("sensor/temp");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "client1");
    }

    #[test]
    fn single_level_wildcard() {
        let mut tree = TopicTree::default();
        tree.add("sensor/+/temp", make_subscriber("client1"));
        let matches = tree.matches("sensor/room1/temp");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn multi_level_wildcard() {
        let mut tree = TopicTree::default();
        tree.add("sensor/#", make_subscriber("client1"));
        let matches = tree.matches("sensor/room1/temp");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn remove_subscription() {
        let mut tree = TopicTree::default();
        tree.add("sensor/temp", make_subscriber("client1"));
        tree.remove("sensor/temp", "client1");
        let matches = tree.matches("sensor/temp");
        assert!(matches.is_empty());
    }

    #[test]
    fn no_match() {
        let mut tree = TopicTree::default();
        tree.add("sensor/temp", make_subscriber("client1"));
        let matches = tree.matches("other/topic");
        assert!(matches.is_empty());
    }
}
