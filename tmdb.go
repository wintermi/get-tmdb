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
	"strings"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/ybbus/httpretry"
)

type TheMovieDB struct {
	APIKey       string
	OutputPath   string
	ExportDate   time.Time
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

type TVSeriesExport struct {
	Id           int64   `json:"id,omitempty"`
	OriginalName string  `json:"original_name,omitempty"`
	Popularity   float64 `json:"popularity,omitempty"`
}

type PeopleExport struct {
	Adult      bool    `json:"adult,omitempty"`
	Id         int64   `json:"id,omitempty"`
	Name       string  `json:"name,omitempty"`
	Popularity float64 `json:"popularity,omitempty"`
}

type CollectionExport struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type TVNetworksExport struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type KeywordsExport struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type CompaniesExport struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// Worker Pool constants
const numWorkers int64 = 60
const chunkSize int64 = 3000

//---------------------------------------------------------------------------------------

// Return New Instance of The Movie DB struct
func NewMovieDB(apiKey string) *TheMovieDB {

	// Calculate the latest date based on the following logic
	//     The export job runs every day starting at around 7:00 AM UTC,
	//     and all files are available by 8:00 AM UTC.
	utc := time.Now().UTC()
	if utc.Hour() < 8 {
		utc = utc.Add(time.Duration(-24) * time.Hour)
	}

	// Initialise New Instance of The Movie DB
	tmdb := new(TheMovieDB)

	tmdb.APIKey = apiKey
	tmdb.ExportDate = utc
	tmdb.DailyExports = map[string]*DailyExport{
		"Movies":      {"Movies", "movie_ids", "movie_ids.json", "", ""},
		"TV Series":   {"TV Series", "tv_series_ids", "tv_series_ids.json", "", ""},
		"People":      {"People", "person_ids", "person_ids.json", "", ""},
		"Collections": {"Collections", "collection_ids", "collection_ids.json", "", ""},
		"TV Networks": {"TV Networks", "tv_network_ids", "tv_network_ids.json", "", ""},
		"Keywords":    {"Keywords", "keyword_ids", "keyword_ids.json", "", ""},
		"Companies":   {"Companies", "production_company_ids", "company_ids.json", "", ""},
	}

	return tmdb
}

//---------------------------------------------------------------------------------------

// Validate or Create the Output Path if it does not exist
func (de DailyExport) String() string {
	return fmt.Sprintf("Media Type: %v, Name: %v", de.MediaType, de.Name)
}

//---------------------------------------------------------------------------------------

// Validate or Create the Output Path if it does not exist
func (tmdb *TheMovieDB) ValidateOutputPath(outputPath string) error {

	// Calculate the Absolute Output Path
	path, err := filepath.Abs(filepath.Join(outputPath, fmt.Sprintf("export_date=%s", tmdb.ExportDate.Format("2006-01-02"))))
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

	return nil
}

//---------------------------------------------------------------------------------------

// Get The Movie DB Daily ID Exports
func (tmdb *TheMovieDB) GetDailyExports() error {

	logger.Info().Msg("Initiating Request to Get Daily ID Exports")

	// Iterate through All of the Entries
	for _, dailyExport := range tmdb.DailyExports {

		logger.Info().Stringer("Exporting", dailyExport).Msg(indent)

		// Make the Export API Request
		var response bytes.Buffer
		err := requests.
			URL("http://files.tmdb.org").
			Pathf("/p/exports/%s.gz", fmt.Sprintf("%s_%s.json", dailyExport.UrlPrefix, tmdb.ExportDate.Format("01_02_2006"))).
			Param("api_key", tmdb.APIKey).
			ToBytesBuffer(&response).
			Fetch(context.Background())
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
		dailyExport.DataFile, _ = filepath.Abs(filepath.Join(tmdb.OutputPath,
			fmt.Sprintf("%s.json", strings.Replace(strings.ToLower(dailyExport.MediaType), " ", "_", -1))))
		err = os.WriteFile(dailyExport.ExportFile, data, 0600)
		if err != nil {
			return fmt.Errorf("Writing Response to File Failed: %w", err)
		}
	}

	logger.Info().Msg("Completed the Daily ID Exports")

	return nil
}

//---------------------------------------------------------------------------------------

// Worker Pool for Concurrent HTTP API Requests
func RequestWorker(url string, path string, apiKey string, jobs <-chan int64, results chan<- *string) {
	// Create a New HTTP Retry Client
	cl := httpretry.NewDefaultClient(
		httpretry.WithMaxRetryCount(20),
		httpretry.WithRetryPolicy(func(statusCode int, err error) bool {
			return statusCode == 429 || err != nil || statusCode >= 500 || statusCode == 0
		}),
		httpretry.WithBackoffPolicy(func(attemptNum int) time.Duration {
			return 100 * time.Millisecond
		}),
	)

	for id := range jobs {
		// Make the API Request
		var response string
		err := requests.
			URL(url).
			Pathf(path, id).
			Param("api_key", apiKey).
			Client(cl).
			ToString(&response).
			Fetch(context.Background())
		if err != nil {
			logger.Error().Err(err).Msg("API Request Failed:")
		}

		results <- &response
	}
}

//---------------------------------------------------------------------------------------

// Close the Worker Pool and Write the Results to the Output File
func CloseWorkerPool(w *bufio.Writer, chunkCount int64, rowCount int64, jobs chan int64, results chan *string) error {
	close(jobs)

	for num := int64(0); num < chunkCount; num++ {
		response := <-results
		if _, err := w.WriteString(fmt.Sprintf("%s\n", *response)); err != nil {
			return fmt.Errorf("Failed Writing to the Output File")
		}
	}

	// Output chunk message to the log
	logger.Info().Int64("Completed Chunk:", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Movies" file and Export the Movie Data
func (tmdb *TheMovieDB) ExportMovieData() error {

	logger.Info().Msg("Initiating Export of Movie Data")

	dailyExport := tmdb.DailyExports["Movies"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the Movies Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Movies Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
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
				go RequestWorker("https://api.themoviedb.org", "/3/movie/%d", tmdb.APIKey, jobs, results)
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
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of Movies Exported", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "TV Series" file and Export the TV Series Data
func (tmdb *TheMovieDB) ExportTVSeriesData() error {

	logger.Info().Msg("Initiating Export of TV Series Data")

	dailyExport := tmdb.DailyExports["TV Series"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the TV Series Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the TV Series Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the TV Series Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go RequestWorker("https://api.themoviedb.org", "/3/tv/%d", tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var tvSeriesExport *TVSeriesExport = new(TVSeriesExport)
		if err := json.Unmarshal(line, &tvSeriesExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the TV Series Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- tvSeriesExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of TV Series Exported", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "People" file and Export the People Data
func (tmdb *TheMovieDB) ExportPeopleData() error {

	logger.Info().Msg("Initiating Export of People Data")

	dailyExport := tmdb.DailyExports["People"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the People Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the People Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the People Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go RequestWorker("https://api.themoviedb.org", "/3/person/%d", tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var peopleExport *PeopleExport = new(PeopleExport)
		if err := json.Unmarshal(line, &peopleExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the People Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- peopleExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of People Exported", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Collection" file and Export the People Data
func (tmdb *TheMovieDB) ExportCollectionData() error {

	logger.Info().Msg("Initiating Export of Collection Data")

	dailyExport := tmdb.DailyExports["Collections"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the Collection Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Collection Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the Collection Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go RequestWorker("https://api.themoviedb.org", "/3/collection/%d", tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var collectionExport *CollectionExport = new(CollectionExport)
		if err := json.Unmarshal(line, &collectionExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the Collection Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- collectionExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of Collection Exported", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "TV Networks" file and Export the People Data
func (tmdb *TheMovieDB) ExportTVNetworksData() error {

	logger.Info().Msg("Initiating Export of TV Networks Data")

	dailyExport := tmdb.DailyExports["TV Networks"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the TV Networks Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the TV Networks Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the TV Networks Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go RequestWorker("https://api.themoviedb.org", "/3/network/%d", tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var tvNetworksExport *TVNetworksExport = new(TVNetworksExport)
		if err := json.Unmarshal(line, &tvNetworksExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the TV Networks Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- tvNetworksExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of TV Networks Exported", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Keywords" file and Export the People Data
func (tmdb *TheMovieDB) ExportKeywordsData() error {

	logger.Info().Msg("Initiating Export of Keywords Data")

	dailyExport := tmdb.DailyExports["Keywords"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the Keywords Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Keywords Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the Keywords Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go RequestWorker("https://api.themoviedb.org", "/3/keyword/%d", tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var keywordsExport *KeywordsExport = new(KeywordsExport)
		if err := json.Unmarshal(line, &keywordsExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the Keywords Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- keywordsExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of Keywords Exported", rowCount).Msg(indent)

	return nil
}

//---------------------------------------------------------------------------------------

// Iterate through the Daily Exports "Companies" file and Export the People Data
func (tmdb *TheMovieDB) ExportCompaniesData() error {

	logger.Info().Msg("Initiating Export of Companies Data")

	dailyExport := tmdb.DailyExports["Companies"]

	//------------------------------------------------------------------
	// Open the Output File
	wf, err := os.Create(dailyExport.DataFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Output File: %w", err)
	}
	defer wf.Close()

	// Ready a Buffered Writer
	w := bufio.NewWriter(wf)
	defer w.Flush()

	// Open the Companies Daily Export IDs File and scan the lines
	rf, err := os.Open(dailyExport.ExportFile)
	if err != nil {
		return fmt.Errorf("Failed to Open the Companies Daily Export IDs File: %w", err)
	}
	defer rf.Close()

	r := bufio.NewScanner(rf)
	r.Split(bufio.ScanLines)

	//------------------------------------------------------------------
	// Setup the Worker Pool for the given chunk size
	var jobs chan int64
	var results chan *string

	//------------------------------------------------------------------
	// Iterate through All of the Companies Export IDs
	var rowCount int64 = 0
	var chunkCount int64 = 0
	for r.Scan() {

		// Start workers if new Chunk
		if chunkCount == 0 {
			jobs = make(chan int64, chunkSize)
			results = make(chan *string, chunkSize)

			for num := int64(0); num < numWorkers; num++ {
				go RequestWorker("https://api.themoviedb.org", "/3/company/%d", tmdb.APIKey, jobs, results)
			}
		}

		// Read the next line of the file
		line := []byte(r.Text())

		// Unmarshal the JSON data contained in the line
		var companiesExport *CompaniesExport = new(CompaniesExport)
		if err := json.Unmarshal(line, &companiesExport); err != nil {
			return fmt.Errorf("Failed to Unmarshal the Companies Export JSON Data: %w", err)
		}

		// Add to the Worker Pool
		jobs <- companiesExport.Id

		chunkCount++
		rowCount++

		// When you reach the max chunk size, wait for the Worker Pool to complete
		// all of the jobs and write the response to the output file
		if chunkCount == chunkSize {
			if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
				return fmt.Errorf("Close Worker Pool Failed: %w", err)
			}
			chunkCount = 0
		}
	}

	// When you reach the max chunk size, wait for the Worker Pool to complete
	// all of the jobs and write the response to the output file
	if chunkCount > 0 {
		if err := CloseWorkerPool(w, chunkCount, rowCount, jobs, results); err != nil {
			return fmt.Errorf("Close Worker Pool Failed: %w", err)
		}
	}

	logger.Info().Int64("Number of Companies Exported", rowCount).Msg(indent)

	return nil
}
