// Copyright 2024, Matthew Winter
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger
var applicationText = "%s 0.1.0%s"
var copyrightText = "Copyright 2024, Matthew Winter\n"
var indent = "..."

var helpText = `
A command line application designed to crawl The Movie DB API following
the The Movie DB API rules, and outputs the results as JSONL files so
you can go forth and experiment with movie data.

Use --help for more details.


USAGE:
    get-tmdb -a API_KEY -o OUTPUT_PATH

ARGS:
`

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, applicationText, filepath.Base(os.Args[0]), "\n")
		fmt.Fprint(os.Stderr, copyrightText)
		fmt.Fprint(os.Stderr, helpText)
		flag.PrintDefaults()
	}

	// Define the Long CLI flag names
	var outputPath = flag.String("o", "", "Output Path  (Required)")
	var tmdbAPIKey = flag.String("a", "", "The Movie DB API Key  (Required)")
	var skipMovies = flag.Bool("skipMovies", false, "Skip Movie Exports")
	var skipTVSeries = flag.Bool("skipTVSeries", false, "Skip TV Series Exports")
	var skipPeople = flag.Bool("skipPeople", false, "Skip People Exports")
	var skipCollections = flag.Bool("skipCollections", false, "Skip Collections Exports")
	var skipTVNetworks = flag.Bool("skipTVNetworks", false, "Skip TV Networks Exports")
	var skipKeywords = flag.Bool("skipKeywords", false, "Skip Keywords Exports")
	var skipProductionCompanies = flag.Bool("skipProductionCompanies", false, "Skip Production Companies Exports")
	var verbose = flag.Bool("v", false, "Output Verbose Detail")

	// Parse the flags
	flag.Parse()

	// Validate the Required Flags
	if *outputPath == "" || *tmdbAPIKey == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Setup Zero Log for Consolo Output
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	logger = zerolog.New(output).With().Timestamp().Logger()
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	zerolog.DurationFieldUnit = time.Millisecond
	zerolog.DurationFieldInteger = true
	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Output Header
	logger.Info().Msgf(applicationText, filepath.Base(os.Args[0]), "")
	logger.Info().Msg("Arguments")
	logger.Info().Str("Output Path", *outputPath).Msg(indent)
	logger.Info().Str("The Movie DB API Key", *tmdbAPIKey).Msg(indent)
	logger.Info().Bool("Skip Movie Exports", *skipMovies).Msg(indent)
	logger.Info().Bool("Skip TV Series Exports", *skipTVSeries).Msg(indent)
	logger.Info().Bool("Skip People Exports", *skipPeople).Msg(indent)
	logger.Info().Bool("Skip Collections Exports", *skipCollections).Msg(indent)
	logger.Info().Bool("Skip TV Networks Exports", *skipTVNetworks).Msg(indent)
	logger.Info().Bool("Skip Keywords Exports", *skipKeywords).Msg(indent)
	logger.Info().Bool("Skip Production Companies Exports", *skipProductionCompanies).Msg(indent)
	logger.Info().Msg("Begin")

	var tmdb *TheMovieDB = NewMovieDB(*tmdbAPIKey)
	if err := tmdb.ValidateOutputPath(*outputPath); err != nil {
		logger.Error().Err(err).Msg("Output Path Validation Failed")
		os.Exit(1)
	}

	if err := tmdb.GetDailyExports(); err != nil {
		logger.Error().Err(err).Msg("Get Daily ID Exports Failed")
		os.Exit(1)
	}

	if !*skipMovies {
		if err := tmdb.ExportMovieData(); err != nil {
			logger.Error().Err(err).Msg("Export Movie Data Failed")
			os.Exit(1)
		}
	}

	if !*skipTVSeries {
		if err := tmdb.ExportTVSeriesData(); err != nil {
			logger.Error().Err(err).Msg("Export TV Series Data Failed")
			os.Exit(1)
		}
	}

	if !*skipPeople {
		if err := tmdb.ExportPeopleData(); err != nil {
			logger.Error().Err(err).Msg("Export People Data Failed")
			os.Exit(1)
		}
	}

	if !*skipCollections {
		if err := tmdb.ExportCollectionData(); err != nil {
			logger.Error().Err(err).Msg("Export Collection Data Failed")
			os.Exit(1)
		}
	}

	if !*skipTVNetworks {
		if err := tmdb.ExportTVNetworksData(); err != nil {
			logger.Error().Err(err).Msg("Export TV Networks Data Failed")
			os.Exit(1)
		}
	}

	if !*skipKeywords {
		if err := tmdb.ExportKeywordsData(); err != nil {
			logger.Error().Err(err).Msg("Export Keywords Data Failed")
			os.Exit(1)
		}
	}

	if !*skipProductionCompanies {
		if err := tmdb.ExportTVNetworksData(); err != nil {
			logger.Error().Err(err).Msg("Export Production Companies Data Failed")
			os.Exit(1)
		}
	}

	logger.Info().Msg("Done!")
}
