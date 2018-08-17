package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/hcl"
	"github.com/google/go-cmp/cmp"
	_ "github.com/snowflakedb/gosnowflake"
)

type QuerySpec struct {
	QueryName          string 			`hcl:",key"`     
	GUID               string        
	Query              string        
	AffectedEnv        []interface{}
	AffectedObjectType []interface{}
	AlertType          []interface{}
	Severity           []interface{}
	Detector           []interface{}
	AffectedObject     []interface{}
	EventTime          []interface{}
	Description        []interface{}
	EventData          []interface{}
}

type QueryConfig struct {
	Query []QuerySpec `hcl:"query_spec"`
}

func fatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getCurrentQueries(db *sql.DB) ([]QuerySpec, error) {
	rows, err := db.Query("SELECT query_spec FROM snowalert.public.snowalert_queries;")
	if err != nil {
		return nil, err
	}
	var qsArray []QuerySpec
	defer rows.Close()
	for rows.Next() {
		var qs string
		err = rows.Scan(&qs)
		fatalError(err)
		var qs2 QuerySpec
		err = json.Unmarshal([]byte(qs), &qs2)
		fatalError(err)
		qsArray = append(qsArray, qs2)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return qsArray, nil
}

func getCurrentConfig(confFile string) (*QueryConfig, error) {
	input, err := ioutil.ReadFile(confFile)
	if err != nil {
		return nil, err
	}
	var qf *QueryConfig
	err = hcl.Unmarshal(input, &qf)
	if err != nil {
		return nil, err
	}
	for index, _ := range qf.Query {
		qf.Query[index].Query = strings.Replace(qf.Query[index].Query, "\n", " ", -1)
	}
	return qf, nil
}

func setDifference(a, b map[string]QuerySpec) []QuerySpec {
	var diff []QuerySpec
	m := make(map[string]bool)
	for key, _ := range b {
		m[key] = true
	}
	for key, value := range a {
		if !m[key] {
			diff = append(diff, value)
		}
	}
	return diff
}

func modifiedQueries(a, b map[string]QuerySpec) []QuerySpec {
	var modified []QuerySpec
	m := make(map[string]bool)
	for key, _ := range b {
		m[key] = true
	}
	for key, value := range a {
		if m[key] && !cmp.Equal(a[key], b[key]) {
			modified = append(modified, value)
			fmt.Println("Diff: ",cmp.Diff(b[key], a[key]),"\n")
		}
	}
	return modified
}

func convertJSON(input QuerySpec) ([]byte, error) {
	buffer := &bytes.Buffer{}
	enc := json.NewEncoder(buffer)
	enc.SetEscapeHTML(false)
	err := enc.Encode(input)
	if err != nil {
		return nil, err
	} 
	output := new(bytes.Buffer)
	err = json.Compact(output, buffer.Bytes())
	if err != nil {
		return nil, err
	}
	return output.Bytes(), err
}

func addQuery(qs QuerySpec, db *sql.DB) error {
	jsonqs, err := convertJSON(qs)
	if err != nil {
		return err
	}
	_, err = db.Exec("insert into snowalert.public.snowalert_queries select parse_json(column1) from values (?);", string(jsonqs))
	return err
}

func removeQuery(qs QuerySpec, db *sql.DB) error {
	_, err := db.Exec("delete from snowalert.public.snowalert_queries where query_spec:GUID = ?", qs.GUID)
	return err
}

func updateQuery(qs QuerySpec, db *sql.DB) error {
	jsonqs, err := convertJSON(qs)
	if err != nil {
		return err
	}
	_, err = db.Exec("update snowalert.public.snowalert_queries set query_spec = ? where query_spec:GUID = ?", string(jsonqs), qs.GUID)
	return err
}

func applyChanges(added, removed, changed []QuerySpec, db *sql.DB) error {
	for _, element := range added {
		err := addQuery(element, db)
		if err != nil {
			return err
		}
	}
	for _, element := range removed {
		err := removeQuery(element, db)
		if err != nil {
			return err
		}		
	}
	for _, element := range changed {
		err := updateQuery(element, db)
		if err != nil {
			return err
		}		
	}
	return nil
}

func processChanges(config, current []QuerySpec) ([]QuerySpec, []QuerySpec, []QuerySpec, error) {
	configMap := make(map[string]QuerySpec)
	currentMap := make(map[string]QuerySpec)
	for _, element := range config {
		configMap[element.GUID] = element
	}
	for _, element := range current {
		currentMap[element.GUID] = element
	}
	return setDifference(configMap, currentMap), setDifference(currentMap, configMap), modifiedQueries(configMap, currentMap), nil
}

func main() {
	// Read in input file
	if len(os.Args) != 3 {
		log.Fatal("Usage: query_helper username filename")
	}
	var confirmation string
	currentConfig, err := getCurrentConfig(os.Args[2])
	fatalError(err)
	db, err := sql.Open("snowflake", os.Args[1] + "@"+os.Getenv("SNOWALERT_ACCOUNT")+"/snowalert?authenticator=externalbrowser&warehouse="+os.Getenv("UPDATE_WAREHOUSE")+"&role="+os.Getenv("UPDATE_ROLE"))
	fatalError(err)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	currentQueries, err := getCurrentQueries(db)
	fatalError(err)
	added, removed, changed, err := processChanges(currentConfig.Query, currentQueries)
	fatalError(err)
	fmt.Println("Added: ", added, "\nRemoved: ", removed, "\nChanged: ", changed)
	fmt.Println("Enter yes to apply changes: ")
	_, err = fmt.Scanln(&confirmation)
	fatalError(err)
	if confirmation == "yes" {
		applyChanges(added, removed, changed, db)
		fmt.Println("Changed applied!\n")
	} else {
		fmt.Println("Changes not applied!\n")
	}
	return
}
