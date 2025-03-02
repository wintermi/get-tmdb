# Get The Movie DB

[![Workflows](https://github.com/wintermi/get-tmdb/workflows/Go%20-%20Build/badge.svg)](https://github.com/wintermi/get-tmdb/actions)
[![Go Report](https://goreportcard.com/badge/github.com/wintermi/get-tmdb)](https://goreportcard.com/report/github.com/wintermi/get-tmdb)
[![License](https://img.shields.io/github/license/wintermi/get-tmdb)](https://github.com/wintermi/get-tmdb/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/wintermi/get-tmdb?include_prereleases)](https://github.com/wintermi/get-tmdb/releases)

## Description

A command line application designed to crawl The Movie DB API following the The Movie DB API rules, and outputs the results as JSONL files so you can go forth and experiment with movie data.

```
USAGE:
    get-tmdb -a API_KEY -o OUTPUT_PATH

ARGS:
  -a string
        The Movie DB API Key  (Required)
  -exportDate string
        Export Date Override
  -justIDs
        Only Get Daily Export IDs
  -o string
        Output Path  (Required)
  -skipCollection
        Skip Collection Data Exports
  -skipCompany
        Skip Company Data Exports
  -skipKeyword
        Skip Keyword Data Exports
  -skipMovie
        Skip Movie Data Exports
  -skipPerson
        Skip Person Data Exports
  -skipTVNetwork
        Skip TV Network Data Exports
  -skipTVSeries
        Skip TV Series Data Exports
  -v    Output Verbose Detail
```

## Example

```
get-tmdb -a "API_KEY" -o "./output"
```

## License

**get-tmdb** is released under the [Apache License 2.0](https://github.com/wintermi/get-tmdb/blob/main/LICENSE) unless explicitly mentioned in the file header.
