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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/carlmjohnson/requests"
)

type TheMovieDB struct {
	APIKey       string
	OutputPath   string
	DailyExports map[string]*DailyExport
}

type DailyExport struct {
	MediaType  string
	UrlPrefix  string
	Name       string
	ExportFile string
	DataFile   string
}

type MovieExport struct {
	Adult         bool    `json:"adult,omitempty"`
	Id            int64   `json:"id,omitempty"`
	OriginalTitle string  `json:"original_title,omitempty"`
	Popularity    float64 `json:"popularity,omitempty"`
	Video         bool    `json:"video,omitempty"`
}

var ctx = context.Background()

//---------------------------------------------------------------------------------------

// Validate or Create the Output Path if it does not exist
func (de DailyExport) String() string {
	return fmt.Sprintf("Media Type: %v, Name: %v", de.MediaType, de.Name)
}

//---------------------------------------------------------------------------------------

// Validate or Create the Output Path if it does not exist
func (tmdb *TheMovieDB) ValidateOutputPath(tmdbAPIKey string, outputPath string) error {

	// Calculate the Absolute Output Path
	path, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("Failed To Get Absolute Output Path: %w", err)
	}

	// Make Sure the Output File Path Exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0700); err != nil {
			return fmt.Errorf("Failed to Create the Output File Path: %w", err)
		}
	}
	tmdb.OutputPath = path
	tmdb.APIKey = tmdbAPIKey

	return nil
}

//---------------------------------------------------------------------------------------

// Get The Movie DB Daily ID Exports
func (tmdb *TheMovieDB) GetDailyExports() error {

	logger.Info().Msg("Initiating Request to Get Daily ID Exports")

	tmdb.DailyExports = map[string]*DailyExport{
		"Movies":               {"Movies", "movie_ids", "", "", ""},
		"TV Series":            {"TV Series", "tv_series_ids", "", "", ""},
		"People":               {"People", "person_ids", "", "", ""},
		"Collections":          {"Collections", "collection_ids", "", "", ""},
		"TV Networks":          {"TV Networks", "tv_network_ids", "", "", ""},
		"Keywords":             {"Keywords", "keyword_ids", "", "", ""},
		"Production Companies": {"Production Companies", "production_company_ids", "", "", ""},
	}

	// Calculate the latest date based on the following logic
	//     The export job runs every day starting at around 7:00 AM UTC,
	//     and all files are available by 8:00 AM UTC.
	utc := time.Now().UTC()
	if utc.Hour() < 8 {
		utc = utc.Add(time.Duration(-24) * time.Hour)
	}

	// Iterate through All of the Entries
	for _, dailyExport := range tmdb.DailyExports {

		dailyExport.Name = fmt.Sprintf("%s_%s.json", dailyExport.UrlPrefix, utc.Format("01_02_2006"))
		logger.Info().Stringer("Exporting", dailyExport).Msg(indent)

		// Make the Export API Request
		var response bytes.Buffer
		err := requests.
			URL("http://files.tmdb.org").
			Pathf("/p/exports/%s.gz", dailyExport.Name).
			Param("api_key", tmdb.APIKey).
			ToBytesBuffer(&response).
			Fetch(ctx)
		if err != nil {
			return fmt.Errorf("TMDB Movie API Request Failed: %w", err)
		}

		// Decompress the response data
		gz, err := gzip.NewReader(&response)
		if err != nil {
			return fmt.Errorf("GZIP Decompress Failed: %w", err)
		}

		data, err := io.ReadAll(gz)
		if err != nil {
			return fmt.Errorf("Reading Response Body Failed: %w", err)
		}

		dailyExport.ExportFile, _ = filepath.Abs(filepath.Join(tmdb.OutputPath, dailyExport.Name))
		dailyExport.DataFile, _ = filepath.Abs(filepath.Join(tmdb.OutputPath, "movies.json"))
		err = os.WriteFile(dailyExport.ExportFile, data, 0600)
		if err != nil {
			return fmt.Errorf("Writing Response to File Failed: %w", err)
		}
	}

	logger.Info().Msg("Completed the Daily ID Exports")

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Movies" file and Export the Movie Data
func MovieWorker(id int64, apiKey string, jobs <-chan int64, results chan<- *string) {
	for job := range jobs {
		// Make the Movie API Request
		var response string
		err := requests.
			URL("https://api.themoviedb.org").
			Pathf("/3/movie/%d", job).
			Param("api_key", apiKey).
			ToString(&response).
			Fetch(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("TMDB Movie API Request Failed:")
		}

		results <- &response
	}
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Movies" file and Export the Movie Data
func CloseMovieChunk(w *bufio.Writer, chunkSize int64, rowCount int64, jobs chan int64, results chan *string) error {
	close(jobs)

	for num := int64(0); num < chunkSize; num++ {
		response := <-results
		if _, err := w.WriteString(fmt.Sprintf("%s\n", *response)); err != nil {
			return fmt.Errorf("Failed Writing to the Output File")
		}
	}

	// Output chunk message to the log
	logger.Info().Int64("Completed Movie Export Chunk:", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Movies" file and Export the Movie Data
func (tmdb *TheMovieDB) ExportMovieData() error {

	logger.Info().Msg("Initiating Export of Movie Data")

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(tmdb.DailyExports["Movies"].DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the Movies Daily Export IDs File and scan the lines
	rf, err := os.Open(tmdb.DailyExports["Movies"].ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Movies Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	const numWorkers int64 = 50
	const chunkSize int64 = 1000
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the Movie Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go MovieWorker(num, tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var movieExport *MovieExport = new(MovieExport)
		if err := json.Unmarshal(line, &movieExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the Movie Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- movieExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseMovieChunk(w, chunkSize, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Movie Chunk Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseMovieChunk(w, chunkSize, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Movie Chunk Failed: %w", err)
		}
		chunkCount = 0
	}

	logger.Info().Int64("Number of Movies Exported", rowCount).Msg(indent)

	return nil
}
