//! Namespace matching — port of upstream's pattern-list + label-selector
//! logic (`MatchNamespace`, `TryParseLabelSelector`, `MatchesLabels`) and the
//! `GlobMatcher` used for `Watcher.ExcludedNamespaces`.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use regex::Regex;

// Kubernetes label name: 1-63 chars, alphanumeric plus _ . -, must
// start/end alphanumeric.
// Literal patterns — validity is covered by the selector tests below.
#[allow(clippy::unwrap_used)]
static LABEL_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$").unwrap());

// Kubernetes label key prefix: DNS subdomain.
// Literal pattern — validity is covered by the selector tests below.
#[allow(clippy::unwrap_used)]
static LABEL_PREFIX_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$")
        .unwrap()
});

// Kubernetes label value: up to 63 chars, same rules (empty allowed).
// Literal pattern — validity is covered by the selector tests below.
#[allow(clippy::unwrap_used)]
static LABEL_VALUE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^([A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?)?$").unwrap());

// "<key> in (<values>)" / "<key> notin (<values>)".
// Literal pattern — validity is covered by the selector tests below.
#[allow(clippy::unwrap_used)]
static SET_BASED_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?<key>\S+)\s+(?<op>in|notin)\s*\((?<values>[^)]*)\)$").unwrap()
});

/// Comma-separated regex list, each must match the *entire* value
/// (upstream: `match.Value.Length == value.Length`). Empty list matches all.
pub fn pattern_list_match(pattern_list: &str, value: &str) -> bool {
    if pattern_list.is_empty() {
        return true;
    }
    pattern_list
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .any(|p| {
            Regex::new(p).is_ok_and(|re| re.find(value).is_some_and(|m| m.len() == value.len()))
        })
}

/// One parsed requirement inside a label selector.
#[derive(Debug, PartialEq)]
enum Requirement {
    /// `key=value` / `key==value` — upstream folds these into matchLabels.
    MatchLabel(String, String),
    /// `key in (a,b)` / `key != v` / `key notin (a,b)`.
    Expression {
        key: String,
        op: SetOp,
        values: Vec<String>,
    },
    /// `key` / `!key`.
    Exists { key: String, negate: bool },
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum SetOp {
    In,
    NotIn,
}

/// A parsed Kubernetes label selector — requirements `ANDed` together.
pub struct LabelSelector(Vec<Requirement>);

impl LabelSelector {
    /// Parse a selector string. `Err(errors)` lists every malformed
    /// requirement; callers must fail closed on error (upstream semantics).
    pub fn parse(raw: &str) -> Result<Self, Vec<String>> {
        if raw.trim().is_empty() {
            return Ok(Self(Vec::new()));
        }
        let requirements = split_requirements(raw);
        if requirements.is_empty() {
            return Err(vec![
                "selector is not empty but contains no requirements".into(),
            ]);
        }

        let mut parsed = Vec::new();
        let mut errors = Vec::new();
        for req in requirements {
            match parse_requirement(&req) {
                Ok(r) => parsed.push(r),
                Err(e) => errors.extend(e),
            }
        }
        if errors.is_empty() {
            Ok(Self(parsed))
        } else {
            Err(errors)
        }
    }

    /// All requirements must hold (AND semantics, like upstream `MatchesLabels`).
    #[must_use]
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.0.iter().all(|r| match r {
            Requirement::MatchLabel(k, v) => labels.get(k) == Some(v),
            Requirement::Expression { key, op, values } => {
                let has = labels.get(key);
                match op {
                    SetOp::In => has.is_some_and(|v| values.contains(v)),
                    SetOp::NotIn => has.is_none_or(|v| !values.contains(v)),
                }
            }
            Requirement::Exists { key, negate } => labels.contains_key(key) != *negate,
        })
    }
}

/// Split on top-level commas (parenthesised groups are kept intact).
fn split_requirements(selector: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in selector.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                let part = selector[start..i].trim();
                if !part.is_empty() {
                    out.push(part.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = selector[start..].trim();
    if !last.is_empty() {
        out.push(last.to_string());
    }
    out
}

fn parse_requirement(req: &str) -> Result<Requirement, Vec<String>> {
    // Set-based first (upstream tries it before equality).
    if let Some(caps) = SET_BASED_RE.captures(req) {
        let key = caps["key"].to_string();
        let op = if &caps["op"] == "in" {
            SetOp::In
        } else {
            SetOp::NotIn
        };
        let values: Vec<String> = caps["values"]
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(String::from)
            .collect();

        if !is_valid_label_key(&key) {
            return Err(vec![format!(
                "invalid label key '{key}' in set-based requirement"
            )]);
        }
        if values.is_empty() {
            return Err(vec![format!(
                "set-based requirement for key '{key}' has no values"
            )]);
        }
        for v in &values {
            if !is_valid_label_value(v) {
                return Err(vec![format!("invalid label value '{v}' for key '{key}'")]);
            }
        }
        return Ok(Requirement::Expression { key, op, values });
    }

    // Inequality `!=`.
    if let Some((k, v)) = req.split_once("!=") {
        let (key, value) = (k.trim().to_string(), v.trim().to_string());
        if !is_valid_label_key(&key) {
            return Err(vec![format!(
                "invalid label key '{key}' in inequality requirement"
            )]);
        }
        if !is_valid_label_value(&value) {
            return Err(vec![format!(
                "invalid label value '{value}' for key '{key}'"
            )]);
        }
        return Ok(Requirement::Expression {
            key,
            op: SetOp::NotIn,
            values: vec![value],
        });
    }

    // Equality `=` or `==`.
    let eq_index = req.find("==").or_else(|| req.find('='));
    if let Some(idx) = eq_index {
        let op_len = if req[idx..].starts_with("==") { 2 } else { 1 };
        let key = req[..idx].trim().to_string();
        let value = req[idx + op_len..].trim().to_string();
        if !is_valid_label_key(&key) {
            return Err(vec![format!(
                "invalid label key '{key}' in equality requirement"
            )]);
        }
        if !is_valid_label_value(&value) {
            return Err(vec![format!(
                "invalid label value '{value}' for key '{key}'"
            )]);
        }
        return Ok(Requirement::MatchLabel(key, value));
    }

    // Existence `key` / `!key`.
    let (negate, key) = req.strip_prefix('!').map_or_else(
        || (false, req.trim().to_string()),
        |rest| (true, rest.trim().to_string()),
    );
    if is_valid_label_key(&key) {
        return Ok(Requirement::Exists { key, negate });
    }

    Err(vec![format!("requirement '{req}' could not be parsed")])
}

fn is_valid_label_key(key: &str) -> bool {
    if key.is_empty() {
        return false;
    }
    let name = match key.split_once('/') {
        Some((prefix, name)) => {
            if prefix.is_empty() || prefix.len() > 253 || !LABEL_PREFIX_RE.is_match(prefix) {
                return false;
            }
            name
        }
        None => key,
    };
    !name.is_empty() && name.len() <= 63 && LABEL_NAME_RE.is_match(name)
}

fn is_valid_label_value(value: &str) -> bool {
    value.len() <= 63 && LABEL_VALUE_RE.is_match(value)
}

/// Combined name-pattern + label-selector matching for a namespace
/// (upstream `MatchNamespace`): OR between the two; both empty → allow all.
pub struct MatchNamespace<'a> {
    patterns: &'a str,
    selector: &'a str,
}

impl<'a> MatchNamespace<'a> {
    #[must_use]
    pub const fn new(patterns: &'a str, selector: &'a str) -> Self {
        Self { patterns, selector }
    }

    /// `ns_labels: None` when the namespace object isn't cached — fail closed
    /// if a selector is configured (upstream `CanBeReflectedToNamespaceCached`).
    #[must_use]
    pub fn matches(&self, ns: &str, ns_labels: Option<&BTreeMap<String, String>>) -> bool {
        let has_patterns = !self.patterns.is_empty();
        let has_selector = !self.selector.is_empty();
        if !has_patterns && !has_selector {
            return true;
        }
        if has_patterns && pattern_list_match(self.patterns, ns) {
            return true;
        }
        if !has_selector {
            return false;
        }
        // Selector configured but namespace object not available → closed.
        ns_labels.is_some_and(|labels| {
            LabelSelector::parse(self.selector).is_ok_and(|s| s.matches(labels))
        })
    }
}

/// Glob patterns for `Watcher.ExcludedNamespaces` — `*`→`.*`, `?`→`.`,
/// everything else escaped, anchored. Upstream lowercases the pattern list.
pub fn parse_glob_patterns(patterns: &str) -> Vec<Regex> {
    patterns
        .to_lowercase()
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .filter_map(|p| {
            let mut re = String::from("^");
            for c in p.chars() {
                match c {
                    '*' => re.push_str(".*"),
                    '?' => re.push('.'),
                    c => re.push_str(&regex::escape(&c.to_string())),
                }
            }
            re.push('$');
            Regex::new(&re).ok()
        })
        .collect()
}

#[must_use]
pub fn is_namespace_excluded(ns: Option<&str>, patterns: &[Regex]) -> bool {
    // Cluster-scoped events (None) are never excluded.
    ns.is_some_and(|ns| !ns.is_empty() && patterns.iter().any(|p| p.is_match(ns)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // --- pattern_list_match ---

    #[test]
    fn empty_pattern_list_matches_everything() {
        assert!(pattern_list_match("", "anything"));
    }

    #[test]
    fn pattern_must_cover_full_value() {
        assert!(pattern_list_match("app-.*", "app-foo"));
        assert!(!pattern_list_match("app-.*", "xapp-foo"));
        assert!(pattern_list_match("app|prod", "app"));
        assert!(pattern_list_match("app|prod", "prod"));
        assert!(!pattern_list_match("app|prod", "staging"));
    }

    #[test]
    fn comma_separated_patterns() {
        assert!(pattern_list_match("app-.*, kube-.*", "kube-system"));
        assert!(!pattern_list_match("app-.*, kube-.*", "monitoring"));
    }

    #[test]
    fn invalid_regex_never_matches() {
        assert!(!pattern_list_match("[invalid", "anything"));
    }

    // --- label selector parsing ---

    #[test]
    fn parses_equality() {
        let s = LabelSelector::parse("team=platform").unwrap();
        assert!(s.matches(&labels(&[("team", "platform")])));
        assert!(!s.matches(&labels(&[("team", "other")])));
        assert!(!s.matches(&labels(&[])));
    }

    #[test]
    fn parses_double_eq() {
        let s = LabelSelector::parse("env==prod").unwrap();
        assert!(s.matches(&labels(&[("env", "prod")])));
    }

    #[test]
    fn parses_inequality() {
        let s = LabelSelector::parse("env!=dev").unwrap();
        assert!(s.matches(&labels(&[("env", "prod")])));
        assert!(!s.matches(&labels(&[("env", "dev")])));
        // Upstream NotIn: missing label still satisfies !=.
        assert!(s.matches(&labels(&[])));
    }

    #[test]
    fn parses_set_based() {
        let s = LabelSelector::parse("env in (prod,staging)").unwrap();
        assert!(s.matches(&labels(&[("env", "staging")])));
        assert!(!s.matches(&labels(&[("env", "dev")])));
        assert!(!s.matches(&labels(&[])));

        let s = LabelSelector::parse("env notin (dev)").unwrap();
        assert!(s.matches(&labels(&[("env", "prod")])));
        assert!(!s.matches(&labels(&[("env", "dev")])));
    }

    #[test]
    fn parses_existence() {
        let s = LabelSelector::parse("team").unwrap();
        assert!(s.matches(&labels(&[("team", "anything")])));
        assert!(!s.matches(&labels(&[])));

        let s = LabelSelector::parse("!team").unwrap();
        assert!(s.matches(&labels(&[])));
        assert!(!s.matches(&labels(&[("team", "x")])));
    }

    #[test]
    fn commas_and_requirements() {
        let s = LabelSelector::parse("team=platform, env in (prod, staging)").unwrap();
        assert!(s.matches(&labels(&[("team", "platform"), ("env", "prod")])));
        assert!(!s.matches(&labels(&[("team", "platform"), ("env", "dev")])));
        assert!(!s.matches(&labels(&[("env", "prod")])));
    }

    #[test]
    fn malformed_selector_fails() {
        assert!(LabelSelector::parse("!!!").is_err());
        assert!(LabelSelector::parse("key in ()").is_err());
        assert!(LabelSelector::parse("-bad-key=v").is_err());
        // A value over 63 chars is invalid.
        let long = "a".repeat(64);
        assert!(LabelSelector::parse(&format!("k={long}")).is_err());
    }

    #[test]
    fn empty_selector_matches_everything() {
        assert!(LabelSelector::parse("").unwrap().matches(&labels(&[])));
        assert!(LabelSelector::parse("  ").unwrap().matches(&labels(&[])));
    }

    // --- MatchNamespace (patterns OR selector; fail-closed on missing ns) ---

    #[test]
    fn match_namespace_open_when_both_empty() {
        let m = MatchNamespace::new("", "");
        assert!(m.matches("anything", None));
    }

    #[test]
    fn match_namespace_or_semantics() {
        let m = MatchNamespace::new("app-.*", "team=platform");
        assert!(m.matches("app-x", Some(&BTreeMap::new()))); // pattern hit
        assert!(m.matches("other", Some(&labels(&[("team", "platform")])))); // selector hit
        assert!(!m.matches("other", Some(&labels(&[("team", "ops")]))));
    }

    #[test]
    fn selector_without_namespace_fails_closed() {
        let m = MatchNamespace::new("", "team=platform");
        assert!(!m.matches("x", None));
    }

    // --- glob exclusion ---

    #[test]
    fn glob_matches() {
        let pats = parse_glob_patterns("kube-*,internal");
        assert!(is_namespace_excluded(Some("kube-system"), &pats));
        assert!(is_namespace_excluded(Some("internal"), &pats));
        assert!(!is_namespace_excluded(Some("monitoring"), &pats));
        // Patterns are lowercased upstream; namespace names are already
        // lowercase by DNS-1123, so compare is consistent.
        // Patterns are lowercased; namespace names are already lowercase —
        // a mixed-case candidate never matches.
        assert!(!is_namespace_excluded(Some("Kube-System"), &pats));
    }

    #[test]
    fn glob_never_excludes_cluster_scoped() {
        let pats = parse_glob_patterns("*");
        assert!(!is_namespace_excluded(None, &pats));
    }
}
