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
	"path/filepath"

	"github.com/hashicorp/hcl"
	"github.com/google/go-cmp/cmp"
	_ "github.com/snowflakedb/gosnowflake"
)

type SuppressionSpec struct {
	SuppressionName    string 			`hcl:",key"`     
	GUID               string        
	Query              string        
}

type SuppressionConfig struct {
	Query []SuppressionSpec `hcl:"suppression_spec"`
}

func fatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getCurrentSuppressions(db *sql.DB) ([]SuppressionSpec, error) {
	rows, err := db.Query("SELECT suppression_spec FROM snowalert.public.suppression_queries;")
	if err != nil {
		return nil, err
	}
	var qsArray []SuppressionSpec
	defer rows.Close()
	for rows.Next() {
		var qs string
		err = rows.Scan(&qs)
		fatalError(err)
		var qs2 SuppressionSpec
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

func getFileConfig(confFile string) ([]SuppressionSpec) {
	input, err := ioutil.ReadFile(confFile)
	if err != nil {
		return nil
	}
	var qf *SuppressionConfig
	err = hcl.Unmarshal(input, &qf)
	if err != nil {
		return nil
	}
	for index, _ := range qf.Query {
		qf.Query[index].Query = strings.Replace(qf.Query[index].Query, "\n", " ", -1)
	}
	return qf.Query
}

func getCurrentConfig(confFolder string) (SuppressionConfig, error) {
	var qf SuppressionConfig
	filepath.Walk(confFolder, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
		} else if filepath.Ext(path) == ".qs" {
			fmt.Println("Loaded file: " + path)
			qf.Query = append(qf.Query, getFileConfig(path)...)
		}
		return nil
	})
	if len(qf.Query) == 0 {
		fmt.Println("No config files loaded, please ensure you're running suppression_helper from the same folder as your config files")
		log.Fatal("Aborting run!")
	}
	return qf, nil
}

func setDifference(a, b map[string]SuppressionSpec) []SuppressionSpec {
	var diff []SuppressionSpec
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

func modifiedSuppressions(a, b map[string]SuppressionSpec) []SuppressionSpec {
	var modified []SuppressionSpec
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

func convertJSON(input SuppressionSpec) ([]byte, error) {
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

func addSuppression(qs SuppressionSpec, db *sql.DB) error {
	jsonqs, err := convertJSON(qs)
	if err != nil {
		return err
	}
	_, err = db.Exec("insert into snowalert.public.suppression_queries select parse_json(column1) from values (?);", string(jsonqs))
	return err
}

func removeSuppression(qs SuppressionSpec, db *sql.DB) error {
	_, err := db.Exec("delete from snowalert.public.suppression_queries where suppression_spec:GUID = ?", qs.GUID)
	return err
}

func updateSuppression(qs SuppressionSpec, db *sql.DB) error {
	jsonqs, err := convertJSON(qs)
	if err != nil {
		return err
	}
	_, err = db.Exec("update snowalert.public.suppression_queries set suppression_spec = ? where suppression_spec:GUID = ?", string(jsonqs), qs.GUID)
	return err
}

func applyChanges(added, removed, changed []SuppressionSpec, db *sql.DB) error {
	for _, element := range added {
		err := addSuppression(element, db)
		if err != nil {
			return err
		}
	}
	for _, element := range removed {
		err := removeSuppression(element, db)
		if err != nil {
			return err
		}		
	}
	for _, element := range changed {
		err := updateSuppression(element, db)
		if err != nil {
			return err
		}		
	}
	return nil
}

func processChanges(config, current []SuppressionSpec) ([]SuppressionSpec, []SuppressionSpec, []SuppressionSpec, error) {
	configMap := make(map[string]SuppressionSpec)
	currentMap := make(map[string]SuppressionSpec)
	for _, element := range config {
		configMap[element.GUID] = element
	}
	for _, element := range current {
		currentMap[element.GUID] = element
	}
	return setDifference(configMap, currentMap), setDifference(currentMap, configMap), modifiedSuppressions(configMap, currentMap), nil
}

func main() {
	// Read in input files
	var confirmation string
	ex, err := os.Executable()
	fatalError(err)
	confFolder := filepath.Dir(ex)
	currentConfig, err := getCurrentConfig(confFolder)
	fatalError(err)
    db, err := sql.Open("snowflake", os.Getenv("UPDATE_USER") + "@"+os.Getenv("SNOWALERT_ACCOUNT")+"/snowalert?authenticator=externalbrowser&warehouse="+os.Getenv("UPDATE_WAREHOUSE")+"&role="+os.Getenv("UPDATE_ROLE"))
	fatalError(err)
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	currentSuppressions, err := getCurrentSuppressions(db)
	fatalError(err)
	added, removed, changed, err := processChanges(currentConfig.Query, currentSuppressions)
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
