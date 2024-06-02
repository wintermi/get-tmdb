# Get The Movie DB

[![Workflows](https://github.com/wintermi/get-tmdb/workflows/Go/badge.svg)](https://github.com/wintermi/get-tmdb/actions/workflows/go.yml)
[![Go Report](https://goreportcard.com/badge/github.com/wintermi/get-tmdb)](https://goreportcard.com/report/github.com/wintermi/get-tmdb)
[![License](https://img.shields.io/github/license/wintermi/get-tmdb.svg)](https://github.com/wintermi/get-tmdb/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/wintermi/get-tmdb?include_prereleases)](https://github.com/wintermi/get-tmdb/releases)

## Description

A command line application designed to crawl The Movie DB API following the The Movie DB API rules, and outputs the results as JSONL files so you can go forth and experiment with movie data.

```
USAGE:
    get-tmdb -a API_KEY -o OUTPUT_PATH

ARGS:
  -a string
        The Movie DB API Key  (Required)
  -o string
        Output Path  (Required)
  -skipCollections
        Skip Collections Exports
  -skipCompanies
        Skip Companies Exports
  -skipKeywords
        Skip Keywords Exports
  -skipMovies
        Skip Movie Exports
  -skipPeople
        Skip People Exports
  -skipTVNetworks
        Skip TV Networks Exports
  -skipTVSeries
        Skip TV Series Exports
  -v    Output Verbose Detail
```

## Example

```
get-tmdb -a "API_KEY" -o "./output"
```

## License

**get-tmdb** is released under the [Apache License 2.0](https://github.com/wintermi/get-tmdb/blob/main/LICENSE) unless explicitly mentioned in the file header.
