# Development

## Build

```fish
cargo fmt --all
cargo test
cargo clippy --all-targets -- -D warnings
```

## Docs

This site is built with [Zensical](https://zensical.org). Source lives in
`docs/`; `zensical.toml` holds the site config.

```shell
uvx zensical serve   # local preview with live reload
uvx zensical build   # static output in site/
```

Pushes to `main` deploy to GitHub Pages via `.github/workflows/docs.yml`.
