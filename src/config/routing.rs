use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum CycleFinderKind {
    #[default]
    Hybrid,
    Dfs,
    Johnson,
    BellmanFord,
}

impl CycleFinderKind {
    pub fn parse_str(raw: &str) -> anyhow::Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "hybrid" => Ok(Self::Hybrid),
            "dfs" => Ok(Self::Dfs),
            "johnson" => Ok(Self::Johnson),
            "bellman-ford" | "bellman_ford" => Ok(Self::BellmanFord),
            other => anyhow::bail!(
                "invalid cycle_finder {other:?} — expected hybrid, dfs, johnson, or bellman-ford"
            ),
        }
    }
}

impl fmt::Display for CycleFinderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hybrid => write!(f, "hybrid"),
            Self::Dfs => write!(f, "dfs"),
            Self::Johnson => write!(f, "johnson"),
            Self::BellmanFord => write!(f, "bellman-ford"),
        }
    }
}


impl<'de> Deserialize<'de> for CycleFinderKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse_str(&raw).map_err(serde::de::Error::custom)
    }
}
